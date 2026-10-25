package gomavlib

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/stretchr/testify/require"
)

// TestNodeWriteDelay 钉住 Node.WriteDelay 的两个要点：
//  1. datagram 端点（UDP）上，同一通道相邻两个报文**真正写出**的间隔不小于设置值；
//  2. 流式端点（TCP）上不生效 —— 一次 Write 不是「一个 UDP 包」，没有包间隔可言。
//
// 为什么不能用 runtime.Gosched：它只是把 goroutine 放回运行队列，不保证经过任何
// 时间（无竞争时可能立即返回），因此给不了内核/网卡排空发送缓冲区的时间。要让
// 「不要连续写出 UDP 包」成立，只能是真正的等待。
//
// 判据取「客户端收到两帧的时间差」而不是 WriteMessageTo 的耗时：后者只把帧塞进
// chWriteTo（无缓冲）就返回，真正的写发生在每通道的 runWriter 协程里。
//
// 本用例只走 message.Message 分支。mavp2p 的生产路径是 WriteFrameTo（frame.Frame
// 分支），由 TestNodeWriteDelayPacesFrameWrites 单独覆盖 —— 两个分支各有一处
// paceDatagram() 调用点，只测其中一个，删掉另一个仍然全绿。
func TestNodeWriteDelay(t *testing.T) {
	for _, tc := range []struct {
		name           string
		serverEndpoint Endpoint
		clientEndpoint Endpoint
		paced          bool
	}{
		{
			name:           "udp datagrams are paced",
			serverEndpoint: &EndpointUDPServer{Address: "127.0.0.1:" + freeUDPPort(t)},
			paced:          true,
		},
		{
			name:           "tcp streams are not paced",
			serverEndpoint: &EndpointTCPServer{Address: "127.0.0.1:" + freeTCPPort(t)},
			paced:          false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			switch ep := tc.serverEndpoint.(type) {
			case *EndpointUDPServer:
				tc.clientEndpoint = &EndpointUDPClient{Address: ep.Address}
			case *EndpointTCPServer:
				tc.clientEndpoint = &EndpointTCPClient{Address: ep.Address}
			}

			gap := measureWriteGap(t, tc.serverEndpoint, tc.clientEndpoint, 1*time.Second,
				func(server *Node, ch *Channel) {
					require.NoError(t, server.WriteMessageTo(ch, testMessage))
					require.NoError(t, server.WriteMessageTo(ch, testMessage))
				})

			if tc.paced {
				require.GreaterOrEqual(t, gap, 500*time.Millisecond,
					"UDP 通道上相邻报文必须按 WriteDelay 间隔写出，实测间隔 %s", gap)
			} else {
				require.Less(t, gap, 500*time.Millisecond,
					"WriteDelay 只应对 datagram 端点生效，TCP 上实测间隔 %s", gap)
			}
		})
	}
}

// TestNodeWriteDelayPacesFrameWrites 覆盖生产路径：WriteFrameTo 走 frame.Frame 分支。
//
// 三件事同时钉住：
//   - 延迟发生在**写成功之后**：首帧必须很快到达，而不是先睡后写；
//   - 延迟发生在**通道自己的 writer goroutine** 里：两次 WriteFrameTo 调用本身必须
//     很快返回，不能把事件循环拖住；
//   - delay=0 是阴性对照：不能只靠「间隔 ≥ 阈值」证明节流，否则环境抖动也能假绿。
func TestNodeWriteDelayPacesFrameWrites(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration
		gap   func(time.Duration) bool
	}{
		{
			name:  "1s delay paces consecutive frames",
			delay: 1 * time.Second,
			gap:   func(g time.Duration) bool { return g >= 500*time.Millisecond },
		},
		{
			name:  "zero delay does not pace",
			delay: 0,
			gap:   func(g time.Duration) bool { return g < 500*time.Millisecond },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			port := freeUDPPort(t)
			serverEP := &EndpointUDPServer{Address: "127.0.0.1:" + port}
			clientEP := &EndpointUDPClient{Address: "127.0.0.1:" + port}

			server, client, ch, arrivals := setupUDPPair(t, serverEP, clientEP, tc.delay)
			defer server.Close()
			defer client.Close()

			// 每帧独立对象：encodeFrame 会就地改写 Message 为 MessageRaw，复用同一个
			// 帧对象会让第二次写入带上第一次的编码结果。
			v2a := pacedTestFrame(t, server)
			v2b := pacedTestFrame(t, server)

			start := time.Now()
			require.NoError(t, server.WriteFrameTo(ch, v2a))
			require.NoError(t, server.WriteFrameTo(ch, v2b))
			callElapsed := time.Since(start)
			require.Less(t, callElapsed, 500*time.Millisecond,
				"WriteFrameTo 不得在调用方睡眠（实测两次调用耗时 %s）", callElapsed)

			var first, second time.Time
			select {
			case first = <-arrivals:
			case <-time.After(3 * time.Second):
				t.Fatal("没有收到第 1 帧")
			}
			require.Less(t, first.Sub(start), 500*time.Millisecond,
				"首帧应在写入后立即到达（睡眠必须发生在本帧写完之后），实测 %s", first.Sub(start))

			select {
			case second = <-arrivals:
			case <-time.After(4 * time.Second):
				t.Fatal("没有收到第 2 帧")
			}

			gap := second.Sub(first)
			require.True(t, tc.gap(gap),
				"delay=%s 时两帧间隔 %s 不符合预期", tc.delay, gap)
		})
	}
}

// TestChannelWriteCountsDrops 钉住写队列满时的计数：chWrite 只有 64 槽且采用
// 非阻塞 select，满时静默丢帧。没有计数就无法区分「网络丢包」与「网关自己丢包」。
//
// 四格：
//   - 队列容量取自生产常量（`writeBufferSize`），不是测试自己拍的数字；
//   - 未满不计、满后逐帧累计（不是只记一次）；
//   - 两个通道的计数互不影响（防止被误做成包级变量）；
//   - ctx 已取消时走 `<-ch.ctx.Done()` 分支，不得把「通道正在关闭」误计成「队列满」。
func TestChannelWriteCountsDrops(t *testing.T) {
	ch := &Channel{
		chWrite: make(chan any, writeBufferSize),
		ctx:     context.Background(),
		label:   "test-channel",
	}
	require.Equal(t, writeBufferSize, cap(ch.chWrite),
		"测试队列容量与生产常量不一致 ⇒ 本用例构造的压力面不是生产的那一个")

	for i := 0; i < writeBufferSize; i++ {
		ch.write(&frame.V2Frame{})
	}
	require.Equal(t, uint64(0), ch.DroppedFrames(), "队列未满时不得计为丢弃")

	for i := 1; i <= 4; i++ {
		ch.write(&frame.V2Frame{})
		require.Equal(t, uint64(i), ch.DroppedFrames(), "队列满后必须逐帧累计丢弃数")
	}

	// 每通道独立计数（字段在 Channel 上，不是包级）。
	other := &Channel{
		chWrite: make(chan any, writeBufferSize),
		ctx:     context.Background(),
		label:   "other-channel",
	}
	for i := 0; i < writeBufferSize; i++ {
		other.write(&frame.V2Frame{})
	}
	other.write(&frame.V2Frame{})
	require.Equal(t, uint64(1), other.DroppedFrames(), "第二个通道的计数起点不是 0")
	require.Equal(t, uint64(4), ch.DroppedFrames(), "两个通道共享了同一个计数器")

	// ctx 取消 + 队列已满 ⇒ 必须走 `<-ch.ctx.Done()` 分支，不得把「通道正在关闭」
	// 计成「队列满」。（队列有空位时 select 会在两个就绪分支里随机选，不能断言不入队；
	// 队列填满后 Done 是唯一就绪分支，判据才是确定的。）
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	closed := &Channel{
		chWrite: make(chan any, writeBufferSize),
		ctx:     canceled,
		label:   "closed-channel",
	}
	for i := 0; i < writeBufferSize; i++ {
		closed.chWrite <- &frame.V2Frame{}
	}
	closed.write(&frame.V2Frame{})
	require.Equal(t, uint64(0), closed.DroppedFrames(),
		"ctx 已取消且队列已满时被计成了队列满 ⇒ 通道关闭期会被误报成丢帧，指向错误的排查方向")
}

// measureWriteGap 起一对节点、等出通道，执行 write 两次，返回两帧到达的时间差。
func measureWriteGap(t *testing.T, serverEP, clientEP Endpoint, delay time.Duration,
	write func(*Node, *Channel),
) time.Duration {
	t.Helper()

	var (
		server   *Node
		client   *Node
		ch       *Channel
		arrivals <-chan time.Time
	)
	if _, ok := clientEP.(*EndpointTCPClient); ok {
		server, client, ch, arrivals = setupTCPPair(t, serverEP, clientEP, delay)
	} else {
		server, client, ch, arrivals = setupUDPPair(t, serverEP, clientEP, delay)
	}
	defer server.Close()
	defer client.Close()

	write(server, ch)

	var first, second time.Time
	select {
	case first = <-arrivals:
	case <-time.After(3 * time.Second):
		t.Fatal("没有收到第 1 帧")
	}
	select {
	case second = <-arrivals:
	case <-time.After(4 * time.Second):
		t.Fatal("没有收到第 2 帧")
	}
	return second.Sub(first)
}

// setupUDPPair 起 UDP 服务端/客户端，返回已建立的通道与到达时间流。
func setupUDPPair(t *testing.T, serverEP, clientEP Endpoint, delay time.Duration,
) (*Node, *Node, *Channel, <-chan time.Time) {
	t.Helper()
	return setupPair(t, serverEP, clientEP, delay)
}

// setupTCPPair 起 TCP 服务端/客户端，返回已建立的通道与到达时间流。
func setupTCPPair(t *testing.T, serverEP, clientEP Endpoint, delay time.Duration,
) (*Node, *Node, *Channel, <-chan time.Time) {
	t.Helper()
	return setupPair(t, serverEP, clientEP, delay)
}

func setupPair(t *testing.T, serverEP, clientEP Endpoint, delay time.Duration,
) (*Node, *Node, *Channel, <-chan time.Time) {
	t.Helper()

	server := &Node{
		Dialect:          testDialect,
		OutVersion:       V2,
		OutSystemID:      10,
		Endpoints:        []Endpoint{serverEP},
		HeartbeatDisable: true,
		WriteDelay:       delay,
	}
	require.NoError(t, server.Initialize())

	client := &Node{
		Dialect:          testDialect,
		OutVersion:       V2,
		OutSystemID:      11,
		Endpoints:        []Endpoint{clientEP},
		HeartbeatDisable: true,
	}
	require.NoError(t, client.Initialize())

	// Events() 是无缓冲通道：不持续取走，节点循环会卡在 pushEvent 上，
	// 连 chWriteTo 都不会处理。故两个节点从一开始就各起一个取事件的协程。
	arrivals := make(chan time.Time, 4)
	go func() {
		for evt := range client.Events() {
			if _, ok := evt.(*EventFrame); ok {
				arrivals <- time.Now()
			}
		}
	}()

	channelOpen := make(chan *Channel, 1)
	go func() {
		for evt := range server.Events() {
			if e, ok := evt.(*EventChannelOpen); ok {
				channelOpen <- e.Channel
			}
		}
	}()

	// 先让服务端认到这个客户端（UDP 服务端只有收到包才会建通道）。
	// 重发而不是只发一次：Node.Initialize 返回时端点监听协程未必已经 bind 好，
	// 首包可能直接丢失（UDP 不重传），只发一次会变成随机的 3 秒超时。
	var ch *Channel
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

waitChannel:
	for {
		require.NoError(t, client.WriteMessageAll(testMessage))
		select {
		case ch = <-channelOpen:
			break waitChannel
		case <-ticker.C:
		case <-deadline:
			server.Close()
			client.Close()
			t.Fatal("服务端没有建立通道")
		}
	}

	return server, client, ch, arrivals
}

// pacedTestFrame 构造一帧可直接由 WriteFrameTo 发出的 V2 帧。
// WriteFrameTo 与 WriteMessageTo 不同：它不负责补校验和（调用方要给全字段），
// 故先 encode 成 MessageRaw，再按该消息的 CRCExtra 计算 Checksum。
func pacedTestFrame(t *testing.T, n *Node) *frame.V2Frame {
	t.Helper()
	fr := &frame.V2Frame{
		SystemID:    10,
		ComponentID: 1,
		Message:     testMessage,
	}
	require.NoError(t, n.encodeFrame(fr))
	mp := n.dialectRW.GetMessage(fr.GetMessage().GetID())
	require.NotNil(t, mp, "测试方言里没有消息 %d", fr.GetMessage().GetID())
	fr.Checksum = fr.GenerateChecksum(mp.CRCExtra())
	return fr
}

func freeUDPPort(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	port := pc.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, pc.Close())
	return fmt.Sprintf("%d", port)
}

func freeTCPPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return fmt.Sprintf("%d", port)
}
