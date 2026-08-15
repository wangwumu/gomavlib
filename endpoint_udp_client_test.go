package gomavlib //nolint:dupl

import (
	"net"
	"testing"

	"github.com/pion/transport/v2/udp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gomavlib/v4/pkg/dialect"
	"github.com/bluenviron/gomavlib/v4/pkg/frame"
	"github.com/bluenviron/gomavlib/v4/pkg/streamwriter"
)

func TestEndpointUDPClient(t *testing.T) {
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:5601")
	require.NoError(t, err)

	ln, err := udp.Listen("udp", addr)
	require.NoError(t, err)
	defer ln.Close()

	go func() { //nolint:dupl
		conn, err2 := ln.Accept()
		require.NoError(t, err2)
		defer conn.Close()

		dialectRW := &dialect.ReadWriter{Dialect: testDialect}
		err2 = dialectRW.Initialize()
		require.NoError(t, err2)

		rw := &frame.ReadWriter{
			ByteReadWriter: conn,
			DialectRW:      dialectRW,
		}
		err2 = rw.Initialize()
		require.NoError(t, err2)

		sw := &streamwriter.Writer{
			FrameWriter: rw.Writer,
			Version:     streamwriter.V2,
			SystemID:    11,
		}
		err2 = sw.Initialize()
		require.NoError(t, err2)

		for i := range 3 {
			var fr frame.Frame
			fr, err2 = rw.Read()
			require.NoError(t, err2)
			require.Equal(t, &frame.V2Frame{
				SequenceNumber: byte(i),
				SystemID:       10,
				ComponentID:    1,
				Message: &MessageHeartbeat{
					Type:           1,
					Autopilot:      2,
					BaseMode:       3,
					CustomMode:     6,
					SystemStatus:   4,
					MavlinkVersion: 5,
				},
				Checksum: fr.GetChecksum(),
			}, fr)

			err2 = sw.Write(&MessageHeartbeat{
				Type:           6,
				Autopilot:      5,
				BaseMode:       4,
				CustomMode:     3,
				SystemStatus:   2,
				MavlinkVersion: 1,
			})
			require.NoError(t, err2)
		}
	}()

	node := &Node{
		Dialect:          testDialect,
		OutVersion:       V2,
		OutSystemID:      10,
		Endpoints:        []Endpoint{&EndpointUDPClient{Address: "127.0.0.1:5601"}},
		HeartbeatDisable: true,
	}
	err = node.Initialize()
	require.NoError(t, err)
	defer node.Close()

	evt := <-node.Events()
	require.Equal(t, &EventChannelOpen{
		Channel: evt.(*EventChannelOpen).Channel,
	}, evt)

	for i := range 3 {
		err = node.WriteMessageAll(&MessageHeartbeat{
			Type:           1,
			Autopilot:      2,
			BaseMode:       3,
			CustomMode:     6,
			SystemStatus:   4,
			MavlinkVersion: 5,
		})
		require.NoError(t, err)

		evt = <-node.Events()
		require.Equal(t, &EventFrame{
			Frame: &frame.V2Frame{
				SequenceNumber: byte(i),
				SystemID:       11,
				ComponentID:    1,
				Message: &MessageHeartbeat{
					Type:           6,
					Autopilot:      5,
					BaseMode:       4,
					CustomMode:     3,
					SystemStatus:   2,
					MavlinkVersion: 1,
				},
				Checksum: evt.(*EventFrame).Frame.GetChecksum(),
			},
			Channel: evt.(*EventFrame).Channel,
		}, evt)
	}
}

func TestEndpointUDPClientDatagramRecovery(t *testing.T) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:5604")
	require.NoError(t, err)
	defer pc.Close()

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		buf := make([]byte, 4096)
		_, clientAddr, err2 := pc.ReadFrom(buf)
		require.NoError(t, err2)

		// first malformed packet (too short)
		_, err2 = pc.WriteTo([]byte{frame.V2MagicByte}, clientAddr)
		require.NoError(t, err2)

		// second bad packet: incompat=0x04 is now a legitimate deviceID high byte
		// (docs/10 §1.4 + docs/11 — parser no longer rejects incompat bit1~7, only bit0
		// SIGNED check remains). The frame parses normally and fails on a wrong checksum.
		_, err2 = pc.WriteTo([]byte{frame.V2MagicByte, 5, 0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, clientAddr)
		require.NoError(t, err2)

		// valid packet, with extra bytes
		_, err2 = pc.WriteTo([]byte{
			0xfd, 0x09, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x00,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x06, 0x05,
			0x04, 0x02, 0x01, 0xb4, 0xde,
			0xff, 0xff, 0xff, 0xff, 0xff,
		}, clientAddr)
		require.NoError(t, err2)

		// valid packet
		_, err2 = pc.WriteTo([]byte{
			0xfd, 0x09, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x00,
			0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x06, 0x05,
			0x04, 0x02, 0x01, 0xb4, 0xde,
		}, clientAddr)
		require.NoError(t, err2)
	}()

	node := &Node{
		Dialect:          testDialect,
		OutVersion:       V2,
		OutSystemID:      10,
		Endpoints:        []Endpoint{&EndpointUDPClient{Address: "127.0.0.1:5604"}},
		HeartbeatDisable: true,
	}
	err = node.Initialize()
	require.NoError(t, err)
	defer node.Close()

	evt := <-node.Events()
	require.Equal(t, &EventChannelOpen{
		Channel: evt.(*EventChannelOpen).Channel,
	}, evt)

	err = node.WriteMessageAll(&MessageHeartbeat{
		Type:           1,
		Autopilot:      2,
		BaseMode:       3,
		CustomMode:     6,
		SystemStatus:   4,
		MavlinkVersion: 5,
	})
	require.NoError(t, err)

	evt = <-node.Events()
	parseErr, ok := evt.(*EventParseError)
	require.True(t, ok)
	require.EqualError(t, parseErr.Error, "packet is too short")

	evt = <-node.Events()
	parseErr, ok = evt.(*EventParseError)
	require.True(t, ok)
	require.EqualError(t, parseErr.Error, "wrong checksum, expected 42c9, got 0000, message id is 0")

	// 注：旧行为下 incompat 高位被拒导致帧头后 7 字节残留产生 "skipped 7 bytes" 事件；
	// 新协议（docs/10 §1.4）放行 incompat bit1~7 后整帧被完整消费，不再有该跳过事件，
	// 下一事件直接是 packet 3 的合法帧。

	evt = <-node.Events()
	fr, ok := evt.(*EventFrame)
	require.True(t, ok)
	require.Equal(t, &EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 0,
			SystemID:       11,
			ComponentID:    1,
			Message: &MessageHeartbeat{
				Type:           6,
				Autopilot:      5,
				BaseMode:       4,
				CustomMode:     3,
				SystemStatus:   2,
				MavlinkVersion: 1,
			},
			Checksum: fr.Frame.GetChecksum(),
		},
		Channel: fr.Channel,
	}, evt)

	evt = <-node.Events()
	parseErr, ok = evt.(*EventParseError)
	require.True(t, ok)
	require.EqualError(t, parseErr.Error, "skipped 5 bytes")

	evt = <-node.Events()
	fr, ok = evt.(*EventFrame)
	require.True(t, ok)
	require.Equal(t, &EventFrame{
		Frame: &frame.V2Frame{
			SequenceNumber: 0,
			SystemID:       11,
			ComponentID:    1,
			Message: &MessageHeartbeat{
				Type:           6,
				Autopilot:      5,
				BaseMode:       4,
				CustomMode:     3,
				SystemStatus:   2,
				MavlinkVersion: 1,
			},
			Checksum: fr.Frame.GetChecksum(),
		},
		Channel: fr.Channel,
	}, evt)

	<-serverDone
}
