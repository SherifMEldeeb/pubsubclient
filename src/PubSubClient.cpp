/*

  PubSubClient.cpp - A simple client for MQTT.
  Nick O'Leary
  http://knolleary.net
*/

#include "PubSubClient.h"

#include <utility>
#include "Arduino.h"

PubSubClient::PubSubClient() : _client{nullptr}, buffer(nullptr), bufferSize(0), keepAlive(0), socketTimeout(0),
                               nextMsgId(0), lastOutActivity(0), lastInActivity(0), pingOutstanding(false), callback(
                nullptr), ip{0, 0, 0, 0}, domain{nullptr}, port{0}, stream(nullptr), _state(MQTT_DISCONNECTED) {
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(Client &client) : _client{nullptr}, buffer(nullptr), bufferSize(0), keepAlive(0),
                                             socketTimeout(0),
                                             nextMsgId(0), lastOutActivity(0), lastInActivity(0),
                                             pingOutstanding(false), callback(
                nullptr), ip{0, 0, 0, 0}, domain{nullptr}, port{0}, stream(nullptr), _state(MQTT_DISCONNECTED) {
    setClient(client);
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(IPAddress addr, uint16_t port, Client &client) : _client{nullptr}, buffer(nullptr),
                                                                            bufferSize(0), keepAlive(0),
                                                                            socketTimeout(0),
                                                                            nextMsgId(0), lastOutActivity(0),
                                                                            lastInActivity(0), pingOutstanding(false),
                                                                            callback(
                                                                                    nullptr), ip{0, 0, 0, 0},
                                                                            domain{nullptr}, port{0}, stream(nullptr),
                                                                            _state(MQTT_DISCONNECTED) {
    setServer(addr, port);
    setClient(client);
    this->stream = nullptr;
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(IPAddress addr, uint16_t port, Client &client, Stream &stream) {
    this->_state = MQTT_DISCONNECTED;
    setServer(addr, port);
    setClient(client);
    setStream(stream);
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(IPAddress addr, uint16_t port, MQTT_CALLBACK_SIGNATURE, Client &client) {
    this->_state = MQTT_DISCONNECTED;
    setServer(addr, port);
    setCallback(callback);
    setClient(client);
    this->stream = NULL;
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(IPAddress addr, uint16_t port, MQTT_CALLBACK_SIGNATURE, Client &client, Stream &stream) {
    this->_state = MQTT_DISCONNECTED;
    setServer(addr, port);
    setCallback(callback);
    setClient(client);
    setStream(stream);
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(uint8_t *ip, uint16_t port, Client &client) {
    this->_state = MQTT_DISCONNECTED;
    setServer(ip, port);
    setClient(client);
    this->stream = NULL;
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(uint8_t *ip, uint16_t port, Client &client, Stream &stream) {
    this->_state = MQTT_DISCONNECTED;
    setServer(ip, port);
    setClient(client);
    setStream(stream);
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(uint8_t *ip, uint16_t port, MQTT_CALLBACK_SIGNATURE callback, Client &client) {
    this->_state = MQTT_DISCONNECTED;
    setServer(ip, port);
    setCallback(callback);
    setClient(client);
    this->stream = NULL;
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(uint8_t *ip, uint16_t port, MQTT_CALLBACK_SIGNATURE callback, Client &client,
                           Stream &stream) {
    this->_state = MQTT_DISCONNECTED;
    setServer(ip, port);
    setCallback(callback);
    setClient(client);
    setStream(stream);
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(const char *domain, uint16_t port, Client &client) {
    this->_state = MQTT_DISCONNECTED;
    setServer(domain, port);
    setClient(client);
    this->stream = NULL;
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(const char *domain, uint16_t port, Client &client, Stream &stream) {
    this->_state = MQTT_DISCONNECTED;
    setServer(domain, port);
    setClient(client);
    setStream(stream);
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(const char *domain, uint16_t port, MQTT_CALLBACK_SIGNATURE callback, Client &client) {
    this->_state = MQTT_DISCONNECTED;
    setServer(domain, port);
    setCallback(callback);
    setClient(client);
    this->stream = NULL;
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(const char *domain, uint16_t port, MQTT_CALLBACK_SIGNATURE callback, Client &client,
                           Stream &stream) {
    this->_state = MQTT_DISCONNECTED;
    setServer(domain, port);
    setCallback(callback);
    setClient(client);
    setStream(stream);
    this->bufferSize = 0;
    setBufferSize(MQTT_MAX_PACKET_SIZE);
    setKeepAlive(MQTT_KEEPALIVE);
    setSocketTimeout(MQTT_SOCKET_TIMEOUT);
}

PubSubClient::PubSubClient(const PubSubClient &other) :
        _client(other._client),
        buffer(nullptr),
        bufferSize(0),
        keepAlive(other.keepAlive),
        socketTimeout(other.socketTimeout),
        nextMsgId(other.nextMsgId),
        lastOutActivity(other.lastOutActivity),
        lastInActivity(other.lastInActivity),
        pingOutstanding(other.pingOutstanding),
        ip(other.ip),
        domain(other.domain),
        port(other.port),
        stream(other.stream),
        _state(other._state) {
    setBufferSize(other.bufferSize);
    strncpy((char *) buffer, (const char *) other.buffer, other.bufferSize);
}

PubSubClient::~PubSubClient() {
    free(this->buffer);
}

bool PubSubClient::connect(const char *id) {
    return connect(id, nullptr, nullptr, 0, 0, 0, 0, 1);
}

bool PubSubClient::connect(const char *id, const char *user, const char *pass) {
    return connect(id, user, pass, nullptr, 0, false, nullptr, true);
}

bool PubSubClient::connect(const char *id, const char *willTopic, uint8_t willQos, bool willRetain,
                           const char *willMessage) {
    return connect(id, NULL, NULL, willTopic, willQos, willRetain, willMessage, true);
}

bool PubSubClient::connect(const char *id, const char *user, const char *pass, const char *willTopic, uint8_t willQos,
                           bool willRetain, const char *willMessage) {
    return connect(id, user, pass, willTopic, willQos, willRetain, willMessage, true);
}

bool PubSubClient::connect(const char *id, const char *user, const char *pass, const char *willTopic, uint8_t willQos,
                           bool willRetain, const char *willMessage, bool cleanSession) {
    if (!connected()) {
        int result = 0;


        if (_client->connected()) {
            Serial.println("MQTT SOCKET CLIENT CONNECTED ALREADY");
            result = 1;
        } else {
            if (domain != nullptr) {
                result = _client->connect(this->domain, this->port);
            } else {
                result = _client->connect(this->ip, this->port);
            }
        }

        if (result == 1) {
            nextMsgId = 1;
            // Leave room in the buffer for header and variable length field
            uint16_t length = MQTT_MAX_HEADER_SIZE;

#if MQTT_VERSION == MQTT_VERSION_3_1
            uint8_t d[9] = {0x00,0x06,'M','Q','I','s','d','p', MQTT_VERSION};
#define MQTT_HEADER_VERSION_LENGTH 9
#elif MQTT_VERSION == MQTT_VERSION_3_1_1
            uint8_t d[7] = {0x00, 0x04, 'M', 'Q', 'T', 'T', MQTT_VERSION};
#define MQTT_HEADER_VERSION_LENGTH 7
#endif
            for (const unsigned char j : d) {
                this->buffer[length++] = j;
            }

            uint8_t v;
            if (willTopic) {
                v = 0x04 | (willQos << 3) | (willRetain << 5);
            } else {
                v = 0x00;
            }
            if (cleanSession) {
                v = v | 0x02;
            }

            if (user != nullptr) {
                v = v | 0x80;

                if (pass != nullptr) {
                    v = v | (0x80 >> 1);
                }
            }
            this->buffer[length++] = v;

            this->buffer[length++] = ((this->keepAlive) >> 8);
            this->buffer[length++] = ((this->keepAlive) & 0xFF);

            CHECK_STRING_LENGTH(length, id)
            length = writeString(id, this->buffer, length);
            if (willTopic) {
                CHECK_STRING_LENGTH(length, willTopic)
                length = writeString(willTopic, this->buffer, length);
                CHECK_STRING_LENGTH(length, willMessage)
                length = writeString(willMessage, this->buffer, length);
            }

            if (user != nullptr) {
                CHECK_STRING_LENGTH(length, user)
                length = writeString(user, this->buffer, length);
                if (pass != nullptr) {
                    CHECK_STRING_LENGTH(length, pass)
                    length = writeString(pass, this->buffer, length);
                }
            }

            write(MQTTCONNECT, this->buffer, length - MQTT_MAX_HEADER_SIZE);

            while (!_client->available()) {
                unsigned long t = millis();
                if (!_client->connected() || t - lastInActivity >= ((int32_t) this->socketTimeout * 1000UL)) {
                    _state = MQTT_CONNECTION_TIMEOUT;
                    Serial.println("MQTT RECIEVE CONNECT ACK TO");
                    _client->stop();
                    return false;
                }
            }
            lastInActivity = lastOutActivity = millis();
            pingOutstanding = false;
            uint8_t llen;
            uint32_t len = readPacket(&llen);

            if (len == 4) {
                if (buffer[3] == 0) {
                    lastInActivity = millis();
                    pingOutstanding = false;
                    _state = MQTT_CONNECTED;
                    return true;
                } else {
                    _state = buffer[3];
                }
            }
            Serial.println("MQTT RECIEVE CONNECT ACK PARSE ERROR");
//            _client->stop();
        } else {
            _state = MQTT_CONNECT_FAILED;
        }
        return false;
    }
    return true;
}

// reads a byte into result
bool PubSubClient::readByte(uint8_t *result) {
    uint32_t previousMillis = millis();
    while (!_client->available()) {
        yield();
        uint32_t currentMillis = millis();
        if (currentMillis - previousMillis >= ((int32_t) this->socketTimeout * 1000)) {
            return false;
        }
    }
    *result = _client->read();
    return true;
}

// reads a byte into result[*index] and increments index
bool PubSubClient::readByte(uint8_t *result, uint16_t *index) {
    uint16_t current_index = *index;
    uint8_t *write_address = &(result[current_index]);
    if (readByte(write_address)) {
        *index = current_index + 1;
        return true;
    }
    return false;
}

uint32_t PubSubClient::readPacket(uint8_t *lengthLength) {
    uint16_t len = 0;
    if (!readByte(this->buffer, &len)) return 0;
    bool isPublish = (this->buffer[0] & 0xF0) == MQTTPUBLISH;
    uint32_t multiplier = 1;
    uint32_t length = 0;
    uint8_t digit = 0;
    uint16_t skip = 0;
    uint32_t start = 0;

    do {
        if (len == 5) {
            // Invalid remaining length encoding - kill the connection
            Serial.println("MQTT PACKET READ Invalid remaining length encoding");
            _state = MQTT_DISCONNECTED;
            _client->stop();
            return 0;
        }
        if (!readByte(&digit)) return 0;
        this->buffer[len++] = digit;
        length += (digit & 127) * multiplier;
        multiplier <<= 7; //multiplier *= 128
    } while ((digit & 128) != 0);
    *lengthLength = len - 1;

    if (isPublish) {
        // Read in topic length to calculate bytes to skip over for Stream writing
        if (!readByte(this->buffer, &len)) return 0;
        if (!readByte(this->buffer, &len)) return 0;
        skip = (this->buffer[*lengthLength + 1] << 8) + this->buffer[*lengthLength + 2];
        start = 2;
        if (this->buffer[0] & MQTTQOS1) {
            // skip message id
            skip += 2;
        }
    }
    uint32_t idx = len;

    for (uint32_t i = start; i < length; i++) {
        if (!readByte(&digit)) return 0;
        if (this->stream) {
            if (isPublish && idx - *lengthLength - 2 > skip) {
                this->stream->write(digit);
            }
        }

        if (len < this->bufferSize) {
            this->buffer[len] = digit;
            len++;
        }
        idx++;
    }

    if (!this->stream && idx > this->bufferSize) {
        len = 0; // This will cause the packet to be ignored.
    }
    return len;
}

bool PubSubClient::loop() {
    if (connected()) {
        if ((millis() - lastInActivity > this->keepAlive * 1000UL) ||
            (millis() - lastOutActivity > this->keepAlive * 1000UL)) {
            if (!pingOutstanding) {
                this->buffer[0] = MQTTPINGREQ;
                this->buffer[1] = 0;
                _client->write(this->buffer, 2);
                lastOutActivity = millis();
                lastInActivity = millis();
                pingOutstanding = true;
            }
        }

        if (_client->available()) {
            uint8_t llen;
            uint16_t len = readPacket(&llen);
            uint16_t msgId = 0;
            uint8_t *payload;
            if (len > 0) {
                lastInActivity = millis();
                uint8_t type = this->buffer[0] & 0xF0;
                if (type == MQTTPUBLISH) {
                    if (callback) {
                        uint16_t tl =
                                (this->buffer[llen + 1] << 8) + this->buffer[llen + 2]; /* topic length in bytes */
                        memmove(this->buffer + llen + 2, this->buffer + llen + 3,
                                tl); /* move topic inside buffer 1 byte to front */
                        this->buffer[llen + 2 + tl] = 0; /* end the topic as a 'C' string with \x00 */
                        char *topic = (char *) this->buffer + llen + 2;
                        // msgId only present for QOS>0
                        if ((this->buffer[0] & 0x06) == MQTTQOS1) {
                            msgId = (this->buffer[llen + 3 + tl] << 8) + this->buffer[llen + 3 + tl + 1];
                            payload = this->buffer + llen + 3 + tl + 2;
                            callback(topic, payload, len - llen - 3 - tl - 2);

                            this->buffer[0] = MQTTPUBACK;
                            this->buffer[1] = 2;
                            this->buffer[2] = (msgId >> 8);
                            this->buffer[3] = (msgId & 0xFF);
                            _client->write(this->buffer, 4);
                            lastOutActivity = millis();

                        } else {
                            payload = this->buffer + llen + 3 + tl;
                            callback(topic, payload, len - llen - 3 - tl);
                        }
                    }
                } else if (type == MQTTPINGREQ) {
                    this->buffer[0] = MQTTPINGRESP;
                    this->buffer[1] = 0;
                    if (_client->write(buffer, 2) != 0) {
                        lastOutActivity = millis();
                        pingOutstanding = false;
                    }
                } else if (type == MQTTPINGRESP)
                    pingOutstanding = false;

            } else if (!connected()) {
                // readPacket has closed the connection
                return false;
            }
        }
        return true;
    }
    return false;
}

bool PubSubClient::publish(const char *topic, const char *payload) {
    return publish(topic, (const uint8_t *) payload, payload ? strnlen(payload, this->bufferSize) : 0, false);
}

bool PubSubClient::publish(const char *topic, const char *payload, bool retained) {
    return publish(topic, (const uint8_t *) payload, payload ? strnlen(payload, this->bufferSize) : 0, retained);
}

bool PubSubClient::publish(const char *topic, const uint8_t *payload, unsigned int pLength) {
    return publish(topic, payload, pLength, false);
}

bool PubSubClient::publish(const char *topic, const uint8_t *payload, unsigned int pLength, bool retained) {
    if (connected()) {
        if (this->bufferSize < MQTT_MAX_HEADER_SIZE + 2 + strnlen(topic, this->bufferSize) + pLength) {
            // Too long
            return false;
        }
        if (buffer != nullptr)
            memset(this->buffer, '\0', this->bufferSize);
        // Leave room in the buffer for header and variable length field
        uint16_t length = MQTT_MAX_HEADER_SIZE;
        length = writeString(topic, this->buffer, length);

        // Add payload
        for (unsigned int i = 0; i < pLength; i++) {
            this->buffer[length++] = payload[i];
        }

        // Write the header
        uint8_t header = MQTTPUBLISH;
        if (retained) {
            header |= 1;
        }
        return write(header, this->buffer, length - MQTT_MAX_HEADER_SIZE);
    }
    return false;
}

bool PubSubClient::publish_P(const char *topic, const char *payload, bool retained) {
    return publish_P(topic, (const uint8_t *) payload, payload ? strnlen(payload, this->bufferSize) : 0, retained);
}

bool PubSubClient::publish_P(const char *topic, const uint8_t *payload, unsigned int pLength, bool retained) {
    uint8_t llen = 0;
    uint8_t digit;
    unsigned int rc = 0;
    uint16_t tlen;
    unsigned int pos = 0;
    unsigned int i;
    uint8_t header;
    unsigned int len;
    int expectedLength;

    if (!connected()) {
        return false;
    }

    tlen = strnlen(topic, this->bufferSize);

    header = MQTTPUBLISH;
    if (retained) {
        header |= 1;
    }
    this->buffer[pos++] = header;
    len = pLength + 2 + tlen;
    do {
        digit = len & 127; //digit = len %128
        len >>= 7; //len = len / 128
        if (len > 0) {
            digit |= 0x80;
        }
        this->buffer[pos++] = digit;
        llen++;
    } while (len > 0);

    pos = writeString(topic, this->buffer, pos);

    rc += _client->write(this->buffer, pos);

    for (i = 0; i < pLength; i++) {
        rc += _client->write((char) pgm_read_byte_near(payload + i));
    }

    lastOutActivity = millis();

    expectedLength = 1 + llen + 2 + tlen + pLength;

    return (rc == expectedLength);
}

bool PubSubClient::beginPublish(const char *topic, unsigned int pLength, bool retained) {
    if (connected()) {
        // Send the header and variable length field
        uint16_t length = MQTT_MAX_HEADER_SIZE;
        length = writeString(topic, this->buffer, length);
        uint8_t header = MQTTPUBLISH;
        if (retained) {
            header |= 1;
        }
        uint16_t hlen = buildHeader(header, this->buffer, pLength + length - MQTT_MAX_HEADER_SIZE);
        uint16_t rc = _client->write(this->buffer + (MQTT_MAX_HEADER_SIZE - hlen),
                                     length - (MQTT_MAX_HEADER_SIZE - hlen));
        lastOutActivity = millis();
        return (rc == (length - (MQTT_MAX_HEADER_SIZE - hlen)));
    }
    return false;
}

int PubSubClient::endPublish() {
    return 1;
}

size_t PubSubClient::write(uint8_t data) {
    lastOutActivity = millis();
    return _client->write(data);
}

size_t PubSubClient::write(const uint8_t *_buffer, size_t size) {
    lastOutActivity = millis();
    return _client->write(_buffer, size);
}

uint16_t PubSubClient::buildHeader(uint8_t header, uint8_t *buf, uint16_t length) {
    // Temporary buffer to hold the encoded length
    uint8_t lenBuf[4];
    uint16_t encodedLength = 0;   // Number of bytes in the encoded length
    uint16_t len = length;

    // Encode the length using MQTT variable-length encoding
    do {
        uint8_t digit = len & 0x7F; // Get the lower 7 bits of len
        len >>= 7;                  // Shift len by 7 bits

        // If there are more bits to encode, set the continuation bit
        if (len > 0) {
            digit |= 0x80; // Set MSB to 1 to indicate more bytes follow
        }

        lenBuf[encodedLength++] = digit; // Store the digit and increment encodedLength
    } while (len > 0);

    // Insert the fixed header at the start of the buffer
    buf[4 - encodedLength] = header;
    for (int i = 0; i < encodedLength; i++) {
        buf[MQTT_MAX_HEADER_SIZE - encodedLength + i] = lenBuf[i];
    }

    // Return the total header size (1 byte for header + llen bytes for length)
    return encodedLength + 1;
}

bool PubSubClient::write(uint8_t header, uint8_t *buf, uint16_t length) {
    uint16_t rc;
    uint16_t hlen = buildHeader(header, buf, length);

#ifdef MQTT_MAX_TRANSFER_SIZE
    uint8_t* writeBuf = buf+(MQTT_MAX_HEADER_SIZE-hlen);
    uint16_t bytesRemaining = length+hlen;  //Match the length type
    uint8_t bytesToWrite;
    bool result = true;
    while((bytesRemaining > 0) && result) {
        delay(0);  // Prevent watchdog crashes
        bytesToWrite = (bytesRemaining > MQTT_MAX_TRANSFER_SIZE)?MQTT_MAX_TRANSFER_SIZE:bytesRemaining;
        rc = _client->write(writeBuf,bytesToWrite);
        result = (rc == bytesToWrite);
        bytesRemaining -= rc;
        writeBuf += rc;
        if (rc != 0) {
            lastOutActivity = millis();
        }
    }
    return result;
#else
    rc = _client->write(buf + (MQTT_MAX_HEADER_SIZE - hlen), length + hlen);
    lastOutActivity = millis();
    return rc == (hlen + length);
#endif
}

bool PubSubClient::subscribe(const char *topic) {
    return subscribe(topic, 0);
}

bool PubSubClient::subscribe(const char *topic, uint8_t qos) {
    size_t topicLength = strnlen(topic, this->bufferSize);
    if (topic == nullptr) {
        return false;
    }
    if (qos > 1) {
        return false;
    }
    if (this->bufferSize < 9 + topicLength) {
        // Too long
        return false;
    }
    if (connected()) {
        // Leave room in the buffer for header and variable length field
        uint16_t length = MQTT_MAX_HEADER_SIZE;
        nextMsgId++;
        if (nextMsgId == 0) {
            nextMsgId = 1;
        }
        if (buffer != nullptr) {
            memset(this->buffer, '\0', this->bufferSize);
            this->buffer[length++] = (nextMsgId >> 8);
            this->buffer[length++] = (nextMsgId & 0xFF);
            length = writeString((char *) topic, this->buffer, length);
            this->buffer[length++] = qos;
            return write(MQTTSUBSCRIBE | MQTTQOS1, this->buffer, length - MQTT_MAX_HEADER_SIZE);
        }
        return false;
    }
    return false;
}

bool PubSubClient::unsubscribe(const char *topic) {
    size_t topicLength = strnlen(topic, this->bufferSize);
    if (topic == 0) {
        return false;
    }
    if (this->bufferSize < 9 + topicLength) {
        // Too long
        return false;
    }
    if (connected()) {
        uint16_t length = MQTT_MAX_HEADER_SIZE;
        nextMsgId++;
        if (nextMsgId == 0) {
            nextMsgId = 1;
        }
        this->buffer[length++] = (nextMsgId >> 8);
        this->buffer[length++] = (nextMsgId & 0xFF);
        length = writeString(topic, this->buffer, length);
        return write(MQTTUNSUBSCRIBE | MQTTQOS1, this->buffer, length - MQTT_MAX_HEADER_SIZE);
    }
    return false;
}

void PubSubClient::disconnect() {
    this->buffer[0] = MQTTDISCONNECT;
    this->buffer[1] = 0;
    _client->write(this->buffer, 2);
    _state = MQTT_DISCONNECTED;
    _client->flush();
    _client->stop();
    pingOutstanding = false;
    lastInActivity = lastOutActivity = millis();
}

//uint16_t PubSubClient::writeString(const char *string, uint8_t *buf, uint16_t pos) {
//    const char *idp = string;
//    uint16_t i = 0;
//    pos += 2;
//    while (*idp) {
//        buf[pos++] = *idp++;
//        i++;
//    }
//    buf[pos - i - 2] = (i >> 8);
//    buf[pos - i - 1] = (i & 0xFF);
//    return pos;
//}

uint16_t PubSubClient::writeString(const char *string, uint8_t *buf, uint16_t pos) {
    const char *idp = string;
    uint16_t i = 0;

    // Calculate the length of the string
    uint16_t str_length = strlen(string);

    // Check if there's enough room for the string and the 2-byte length
    if (pos + 2 + str_length > this->bufferSize) {
        // Not enough space to write the string
        return pos; // Return an error code or handle the error
    }

    // Reserve space for the 2-byte length
    pos += 2;

    // Copy the string to the buffer
    while ((*idp) != 0) {
        buf[pos++] = *idp++;
        i++;
    }

    // Store the length of the string in the first two bytes
    buf[pos - i - 2] = (i >> 8);    // High byte of the string length
    buf[pos - i - 1] = (i & 0xFF);  // Low byte of the string length

    return pos; // Return the new position
}

bool PubSubClient::connected() {
    if (_client == nullptr)
        return false;
    const bool rc = _client->connected() == 1;
    if (!rc) {
        // millis() - max(lastInActivity,lastOutActivity);
        if (this->_state == MQTT_CONNECTED) {
            this->_state = MQTT_CONNECTION_LOST;
            // Serial.println("MQTT Connection for some reason lost");
            // _client->flush();
            // _client->stop();
        }
        return false;
    }
    _state = MQTT_CONNECTED;
    return true;
}

PubSubClient &PubSubClient::setServer(uint8_t *_ip, uint16_t _port) {
    IPAddress addr(_ip[0], _ip[1], _ip[2], _ip[3]);
    return setServer(addr, port);
}

PubSubClient &PubSubClient::setServer(const IPAddress &_ip, uint16_t _port) {
    this->ip = _ip;
    this->port = _port;
    this->domain = nullptr;
    return *this;
}

PubSubClient &PubSubClient::setServer(const char *_domain, uint16_t _port) {
    this->domain = _domain;
    this->port = _port;
    return *this;
}

PubSubClient &PubSubClient::setCallback(MQTT_CALLBACK_SIGNATURE handler) {
    this->callback = std::move(handler);
    return *this;
}

PubSubClient &PubSubClient::setClient(Client &client) {
    this->_client = &client;
    return *this;
}

PubSubClient &PubSubClient::setStream(Stream &_stream) {
    this->stream = &_stream;
    return *this;
}

int PubSubClient::state() const {
    return this->_state;
}

bool PubSubClient::setBufferSize(uint16_t size) {
    if (size == 0) {
        // Cannot set it back to 0
        return false;
    }
    if (this->bufferSize == 0) {
        this->buffer = (uint8_t *) malloc(size);
    } else {
        auto *newBuffer = (uint8_t *) realloc(this->buffer, size);
        if (newBuffer != nullptr) {
            this->buffer = newBuffer;
        } else {
            return false;
        }
    }
    this->bufferSize = size;
    memset(buffer, 0, size);
    return (this->buffer != nullptr);
}

uint16_t PubSubClient::getBufferSize() const {
    return this->bufferSize;
}

PubSubClient &PubSubClient::setKeepAlive(uint16_t _keepAlive) {
    this->keepAlive = _keepAlive;
    return *this;
}

PubSubClient &PubSubClient::setSocketTimeout(uint16_t timeout) {
    this->socketTimeout = timeout;
    return *this;
}
