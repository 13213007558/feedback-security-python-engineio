import pytest

from engineio import packet
from engineio import payload


class TestPayload:
    def test_encode_empty_payload(self):
        p = payload.Payload()
        assert p.packets == []
        assert p.encode() == ''

    def test_decode_empty_payload(self):
        p = payload.Payload(encoded_payload='')
        assert p.encode() == ''

    def test_encode_payload_text(self):
        pkt = packet.Packet(packet.MESSAGE, data='abc')
        p = payload.Payload([pkt])
        assert p.packets == [pkt]
        assert p.encode() == '4abc'

    def test_encode_payload_text_multiple(self):
        pkt = packet.Packet(packet.MESSAGE, data='abc')
        pkt2 = packet.Packet(packet.MESSAGE, data='def')
        p = payload.Payload([pkt, pkt2])
        assert p.packets == [pkt, pkt2]
        assert p.encode() == '4abc\x1e4def'

    def test_encode_payload_binary(self):
        pkt = packet.Packet(packet.MESSAGE, data=b'\x00\x01\x02')
        p = payload.Payload([pkt])
        assert p.packets == [pkt]
        assert p.encode() == 'bAAEC'

    def test_encode_payload_binary_multiple(self):
        pkt = packet.Packet(packet.MESSAGE, data=b'\x00\x01\x02')
        pkt2 = packet.Packet(packet.MESSAGE, data=b'\x03\x04\x05\x06')
        p = payload.Payload([pkt, pkt2])
        assert p.packets == [pkt, pkt2]
        assert p.encode() == 'bAAEC\x1ebAwQFBg=='

    def test_encode_payload_text_binary_multiple(self):
        pkt = packet.Packet(packet.MESSAGE, data='abc')
        pkt2 = packet.Packet(packet.MESSAGE, data=b'\x03\x04\x05\x06')
        p = payload.Payload([pkt, pkt2, pkt2, pkt])
        assert p.packets == [pkt, pkt2, pkt2, pkt]
        assert p.encode() == '4abc\x1ebAwQFBg==\x1ebAwQFBg==\x1e4abc'

    def test_encode_jsonp_payload(self):
        pkt = packet.Packet(packet.MESSAGE, data='abc')
        p = payload.Payload([pkt])
        assert p.packets == [pkt]
        assert p.encode(jsonp_index=233) == '___eio[233]("4abc");'

    def test_decode_jsonp_payload(self):
        p = payload.Payload(encoded_payload='d=4abc')
        assert p.encode() == '4abc'

    def test_decode_invalid_payload(self):
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='bad payload')

    def test_decode_multi_payload_with_too_many_packets(self):
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='4abc\x1e4def\x1e' * 9 + '6')

    def test_decode_too_many_packets_custom_limit(self):
        with pytest.raises(ValueError) as excinfo:
            payload.Payload(encoded_payload='4a\x1e4b', max_decode_packets=1)
        assert 'packets' in str(excinfo.value)
        # the class-level default is not affected by the instance override
        assert payload.Payload.max_decode_packets == 16
        p = payload.Payload(encoded_payload='4a\x1e4b')
        assert len(p.packets) == 2

    def test_decode_too_many_packets_class_attribute_compat(self):
        # changing the class attribute keeps working for code that does
        # not use the per-instance configuration
        payload.Payload.max_decode_packets = 1
        try:
            with pytest.raises(ValueError):
                payload.Payload(encoded_payload='4a\x1e4b')
            # an instance-level override takes precedence over the class
            # attribute
            p = payload.Payload(encoded_payload='4a\x1e4b',
                                max_decode_packets=2)
            assert len(p.packets) == 2
        finally:
            payload.Payload.max_decode_packets = 16

    def test_decode_empty_payload_with_limits(self):
        p = payload.Payload(encoded_payload='', max_decode_packets=1,
                            max_decoded_packet_size=1)
        assert p.packets == []

    def test_decode_text_packet_size_limit(self):
        p = payload.Payload(encoded_payload='4abc',
                            max_decoded_packet_size=3)
        assert p.packets[0].data == 'abc'
        with pytest.raises(ValueError) as excinfo:
            payload.Payload(encoded_payload='4abcd',
                            max_decoded_packet_size=3)
        assert 'bytes' in str(excinfo.value)

    def test_decode_text_packet_size_limit_multibyte(self):
        # 'é' is one character but two UTF-8 bytes; the limit applies to
        # the UTF-8 byte size of the packet data
        p = payload.Payload(encoded_payload='4é',
                            max_decoded_packet_size=2)
        assert p.packets[0].data == 'é'
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='4é',
                            max_decoded_packet_size=1)
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='4éa',
                            max_decoded_packet_size=2)

    def test_decode_binary_packet_size_limit(self):
        encoded = packet.Packet(packet.MESSAGE, data=b'\x00\x01\x02').encode(
            b64=True)
        p = payload.Payload(encoded_payload=encoded,
                            max_decoded_packet_size=3)
        assert p.packets[0].data == b'\x00\x01\x02'
        with pytest.raises(ValueError) as excinfo:
            payload.Payload(encoded_payload=encoded,
                            max_decoded_packet_size=2)
        assert 'bytes' in str(excinfo.value)

    def test_decode_binary_packet_size_limit_padding(self):
        # the decoded size calculation accounts for base64 padding, so
        # packets that decode to exactly the limit are accepted
        for data in [b'\x00', b'\x00\x01', b'\x00\x01\x02']:
            encoded = packet.Packet(packet.MESSAGE, data=data).encode(
                b64=True)
            p = payload.Payload(encoded_payload=encoded,
                                max_decoded_packet_size=len(data))
            assert p.packets[0].data == data
            with pytest.raises(ValueError):
                payload.Payload(encoded_payload=encoded,
                                max_decoded_packet_size=len(data) - 1)

    def test_decode_large_binary_packet_within_limit(self):
        data = bytes(range(256)) * 100
        encoded = packet.Packet(packet.MESSAGE, data=data).encode(b64=True)
        p = payload.Payload(encoded_payload=encoded,
                            max_decoded_packet_size=len(data))
        assert p.packets[0].data == data

    def test_decode_jsonp_packet_size_limit(self):
        # the limit applies after the JSONP form encoding is expanded
        p = payload.Payload(encoded_payload='d=4abc',
                            max_decoded_packet_size=3)
        assert p.packets[0].data == 'abc'
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='d=4abc',
                            max_decoded_packet_size=2)

    def test_decode_jsonp_multibyte_packet_size_limit(self):
        # %C3%A9 expands to 'é', which is two UTF-8 bytes
        p = payload.Payload(encoded_payload='d=4%C3%A9',
                            max_decoded_packet_size=2)
        assert p.packets[0].data == 'é'
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='d=4%C3%A9',
                            max_decoded_packet_size=1)

    def test_decode_separator_boundary_with_limits(self):
        # a payload that ends exactly at a packet boundary is accepted
        p = payload.Payload(encoded_payload='4ab\x1e4cd',
                            max_decode_packets=2, max_decoded_packet_size=2)
        assert [pkt.data for pkt in p.packets] == ['ab', 'cd']
        # a trailing separator produces an empty packet, which is invalid
        # independently of the configured limits
        with pytest.raises(ValueError):
            payload.Payload(encoded_payload='4ab\x1e', max_decode_packets=2,
                            max_decoded_packet_size=100)

    def test_decode_no_decoded_size_limit_by_default(self):
        p = payload.Payload(encoded_payload='4' + 'a' * 1000000)
        assert len(p.packets[0].data) == 1000000
