import urllib

from . import packet


class Payload:
    """Engine.IO payload.

    :param packets: a list of packets to include in the payload.
    :param encoded_payload: an encoded payload to decode.
    :param max_decode_packets: the maximum number of packets that are
                               accepted when decoding a payload. If not
                               given, the ``max_decode_packets`` class
                               attribute is used.
    :param max_decoded_packet_size: the maximum size that is accepted for
                                    the decoded data of each packet in the
                                    payload, given in bytes. For text
                                    packets this is the size of the UTF-8
                                    encoded packet data, and for base64
                                    encoded binary packets it is the size
                                    of the decoded binary data. If not
                                    given, the ``max_decoded_packet_size``
                                    class attribute is used, which defaults
                                    to no limit.
    """
    max_decode_packets = 16
    max_decoded_packet_size = None

    def __init__(self, packets=None, encoded_payload=None,
                 max_decode_packets=None, max_decoded_packet_size=None):
        self.packets = packets or []
        if max_decode_packets is not None:
            self.max_decode_packets = max_decode_packets
        if max_decoded_packet_size is not None:
            self.max_decoded_packet_size = max_decoded_packet_size
        if encoded_payload is not None:
            self.decode(encoded_payload)

    def encode(self, jsonp_index=None):
        """Encode the payload for transmission."""
        encoded_payload = ''
        for pkt in self.packets:
            if encoded_payload:
                encoded_payload += '\x1e'
            encoded_payload += pkt.encode(b64=True)
        if jsonp_index is not None:
            encoded_payload = '___eio[' + \
                              str(jsonp_index) + \
                              ']("' + \
                              encoded_payload.replace('"', '\\"') + \
                              '");'
        return encoded_payload

    def decode(self, encoded_payload):
        """Decode a transmitted payload."""
        self.packets = []

        if len(encoded_payload) == 0:
            return

        # JSONP POST payload starts with 'd='
        if encoded_payload.startswith('d='):
            encoded_payload = urllib.parse.parse_qs(
                encoded_payload)['d'][0]

        encoded_packets = encoded_payload.split('\x1e')
        if len(encoded_packets) > self.max_decode_packets:
            raise ValueError('Too many packets in payload')
        if self.max_decoded_packet_size is not None:
            for encoded_packet in encoded_packets:
                self._check_decoded_packet_size(encoded_packet)
        self.packets = [packet.Packet(encoded_packet=encoded_packet)
                        for encoded_packet in encoded_packets]

    def _check_decoded_packet_size(self, encoded_packet):
        """Check the decoded size of an encoded packet against the
        configured limit, before the packet is decoded.

        The limit is always measured in bytes. For text packets it applies
        to the UTF-8 encoded packet data, and for base64 encoded binary
        packets to the decoded binary data. In both cases the packet type
        character is not included in the size.
        """
        limit = self.max_decoded_packet_size
        if encoded_packet.startswith('b'):
            # base64 encoded binary packet: the exact size of the decoded
            # data can be calculated from the length of the base64 string
            # and its padding, so the limit can be enforced without
            # decoding (and allocating) the binary data
            b64_data = encoded_packet[1:]
            decoded_size = 3 * (len(b64_data) // 4)
            if len(b64_data) % 4 == 0:
                decoded_size -= len(b64_data) - len(b64_data.rstrip('='))
            if decoded_size > limit:
                raise ValueError(
                    'Decoded packet size exceeds the limit of %d bytes' %
                    limit)
        else:
            # text packet: the UTF-8 byte size of the packet data is at
            # least its character count, so a character count above the
            # limit can be rejected right away, and only data with a valid
            # character count needs to be encoded to obtain its exact byte
            # size
            data = encoded_packet[1:]
            if len(data) > limit or len(data.encode('utf-8')) > limit:
                raise ValueError(
                    'Decoded packet size exceeds the limit of %d bytes' %
                    limit)
