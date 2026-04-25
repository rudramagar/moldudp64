#include "application.h"
#include "yaml_config.h"
#include "socket.h"
#include "decoder.h"
#include "recovery.h"

#include <cstdio>
#include <cstdint>
#include <string>
#include <cstring>
#include <cerrno>
#include <vector>

Application::Application()
: max_messages(0),
  verbose(false),
  has_type_filter(false),
  has_start_seq(false),
  start_seq(0),
  enable_recovery(false),
  config_path("config/config.yaml") {
    std::memset(type_allowed, 0, sizeof(type_allowed));
}

void Application::set_max_messages(uint64_t value) {
    max_messages = value;
}

void Application::set_verbose(bool value) {
    verbose = value;
}

void Application::set_type_filter(char type) {
    has_type_filter = true;
    type_allowed[(unsigned char)type] = true;
}

void Application::set_start_seq(uint64_t value) {
    has_start_seq = true;
    start_seq = value;
}

void Application::set_enable_recovery(bool value) {
    enable_recovery = value;
}

void Application::set_config_path(const char* path) {
    if (path != 0 && path[0] != '\0') {
        config_path = path;
    }
}

void Application::set_decode_file(const char* path) {
    if (path != 0 && path[0] != '\0') {
        decode_file_path = path;
    }
}

static void check_sequence_gap(const MoldHeader& header,
                               std::string& current_session,
                               bool& joined, uint64_t& expected_seq) {

    if (!joined) {
        current_session = header.session;
        expected_seq = header.sequence_number;
        joined = true;
        return;
    }

    if (header.session != current_session) {
        current_session = header.session;
        expected_seq = header.sequence_number;
        return;
    }

    if (header.sequence_number > expected_seq) {
        uint64_t gap_count = header.sequence_number - expected_seq;
        std::printf(">> GAP DETECT: ExpectedSequence=%llu, Received=%llu, TotalMissing=%llu\n",
                    (unsigned long long)expected_seq,
                    (unsigned long long)header.sequence_number,
                    (unsigned long long)gap_count);
        return;
    }
}

// Read first valid MoldHeader and return SessionID for -s/-g 
static bool get_session_id(const AppConfig& cfg, char session[10],
                           std::string& session_value) {

    std::memset(session, ' ', 10);
    session_value.clear();

    Socket sock;
    if (!sock.connect_socket(cfg.mcast_ip, cfg.mcast_port, cfg.interface_ip, cfg.mcast_source_ip)) {
        std::printf("Failed to connect multicast socket\n");
        return false;
    }

    sock.set_receive_buffer(4 * 1024 * 1024);
    const int udp_packet_capacity = 64 * 1024;
    uint8_t buffer[udp_packet_capacity];

    while (1) {
        int bytes = sock.receive_bytes(buffer, udp_packet_capacity);
        if (bytes <= 0) {
            continue;
        }

        MoldHeader header;
        if (!parse_mold_header(buffer, bytes, &header)) {
            continue;
        }

        session_value = header.session;

        std::memset(session, ' ', 10);
        size_t session_len = header.session.size();
        if (session_len > 10) {
            session_len = 10;
        }

        std::memcpy(session, header.session.data(), session_len);

        sock.close();
        return true;
    }
}

// Decode MoldUDP Packets
static uint16_t decode_packet_messages(const uint8_t* buffer, int bytes,
                                      const AppConfig& cfg, bool has_type_filter,
                                      const bool type_allowed[256],
                                      uint64_t& decoded_count,
                                      uint64_t max_messages_limit, bool& stop_now, bool verbose) {

    stop_now = false;

    MoldHeader header;
    if (!parse_mold_header(buffer, bytes, &header)) {
        return 0;
    }

    int offset = 10 + 8 + 2;
    uint16_t remaining = (uint16_t)header.message_count;
    uint16_t index = 0;
    const uint8_t* msg = 0;
    uint16_t msg_len = 0;
    uint16_t processed = 0;

    while (next_mold_message(buffer, bytes, &offset, &remaining, &msg, &msg_len)) {
        uint64_t seq = header.sequence_number + (uint64_t)index;
        index++;

        bool allow_print = true;
        if (has_type_filter) {
            allow_print = type_allowed[(unsigned char)msg[0]];
        }

        if (allow_print) {
            decode_itch_message(msg, msg_len, cfg, header.session, seq, (uint16_t)header.message_count, verbose);
        }

        decoded_count++;
        processed++;

        if (max_messages_limit != 0 && decoded_count >= max_messages_limit) {
            stop_now = true;
            return processed;
        }
    }
    return processed;
}

// Gap-fill
static uint64_t gap_fill(Rerequester& rr, const char session[10],
                         uint64_t start_seq, uint64_t gap_count,
                         uint16_t max_per_request, const AppConfig& cfg,
                         bool has_type_filter, const bool type_allowed[256],
                         uint64_t& decoded_count, uint64_t max_messages,
                         bool& stop_now, bool verbose) {

    const int udp_packet_capacity = 64 * 1024;
    uint8_t rxbuf[udp_packet_capacity];
    
    uint64_t recovered = 0;
    uint64_t current_seq = start_seq;
    uint64_t remaining = gap_count;

    while (remaining > 0) {
        uint16_t req_count = max_per_request;

        if (remaining < (uint64_t)max_per_request) {
            req_count = (uint16_t)remaining;
        }

        if (!rr.send_request(session, current_seq, req_count)) {
            break;
        }

        uint64_t got_in_chunk = 0;
        int timeouts = 0;

        while (got_in_chunk < (uint64_t)req_count) {
            int recv_bytes = rr.receive_packet(rxbuf, udp_packet_capacity);
            if (recv_bytes <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    timeouts++;
                    if (timeouts >= 3) {
                        break;
                    }
                    continue;
                }
                std::printf("Recovery recv error errno=%d\n", errno);
                break;
            }

            bool local_stop = false;
            uint16_t processed = decode_packet_messages(rxbuf, recv_bytes, cfg, has_type_filter,
                                                       type_allowed, decoded_count, max_messages,
                                                       local_stop, verbose);

            got_in_chunk += (uint64_t)processed;

            if (local_stop) {
                stop_now = true;
                return recovered + got_in_chunk;
            }
        }

        if (got_in_chunk == 0) {
            break;
        }

        recovered += got_in_chunk;
        current_seq += got_in_chunk;

        if (got_in_chunk >= remaining) {
            remaining = 0;
        } else {
            remaining -= got_in_chunk;
        }
    }

    return recovered;
}

// Decode raw file 
static int decode_file(const std::string& file_path,
                       const AppConfig& cfg,
                       bool has_type_filter,
                       const bool type_allowed[256],
                       uint64_t max_messages,
                       bool verbose) {
 
    std::FILE* file = std::fopen(file_path.c_str(), "rb");
    if (file == 0) {
        std::printf("ERROR: Failed to open raw file: %s\n", file_path.c_str());
        return 1;
    }
 
    std::fseek(file, 0, SEEK_END);
    long file_size = std::ftell(file);
    std::fseek(file, 0, SEEK_SET);
    if (file_size <= 0) {
        std::printf("INFO: Raw file is empty: %s\n", file_path.c_str());
        std::fclose(file);
        return 1;
    }
 
    std::vector<uint8_t> file_bytes((size_t)file_size);
    size_t got = std::fread(&file_bytes[0], 1, (size_t)file_size, file);
    std::fclose(file);
    if (got != (size_t)file_size) {
        std::printf("ERROR: Failed to read raw file fully: got %zu of %ld\n",
                    got, file_size);
        return 1;
    }
 
    const int    mold_header_len = 10 + 8 + 2;
    const size_t total_bytes     = file_bytes.size();
    size_t       cursor          = 0;
    uint64_t     decoded_count   = 0;
    uint64_t     packet_count    = 0;
 
    while (cursor < total_bytes) {
        if (cursor + 4 > total_bytes) {
            std::printf("Truncated length prefix at offset %zu\n", cursor);
            break;
        }
        uint32_t packet_length =
              ((uint32_t)file_bytes[cursor + 0])
            | ((uint32_t)file_bytes[cursor + 1] <<  8)
            | ((uint32_t)file_bytes[cursor + 2] << 16)
            | ((uint32_t)file_bytes[cursor + 3] << 24);
        cursor += 4;
 
        if (packet_length < (uint32_t)mold_header_len) {
            std::printf("Packet too short at offset %zu: len=%u\n",
                        cursor - 4, (unsigned)packet_length);
            break;
        }
        if (cursor + (size_t)packet_length > total_bytes) {
            std::printf("Truncated packet at offset %zu: need %u have %zu\n",
                        cursor - 4, (unsigned)packet_length,
                        total_bytes - cursor);
            break;
        }
 
        const uint8_t* packet_start = &file_bytes[cursor];
 
        MoldHeader header;
        if (!parse_mold_header(packet_start, (int)packet_length, &header)) {
            std::printf("Bad MoldUDP header at offset %zu\n", cursor);
            break;
        }
 
        if (verbose) {
            std::printf("[#PACKET %llu] Length=%u\n",
                        (unsigned long long)(packet_count + 1),
                        (unsigned)packet_length);
        }
 
        // EndSession
        if ((uint16_t)header.message_count == 0xFFFF) {
            char sess10[11];
            std::memset(sess10, ' ', 10);
            size_t n = header.session.size();
            if (n > 10) {
                n = 10;
            }
            if (n) {
                std::memcpy(sess10, header.session.data(), n);
            }
            sess10[10] = '\0';
            std::printf(">> {'%.*s', %llu, %u}\n",
                        10, sess10,
                        (unsigned long long)header.sequence_number, 65535u);
            cursor += (size_t)packet_length;
            packet_count++;
            continue;
        }
 
        if (header.message_count == 0) {
            cursor += (size_t)packet_length;
            packet_count++;
            continue;
        }
 
        // Full packet
        bool stop_now = false;
        decode_packet_messages(packet_start, (int)packet_length, cfg,
                               has_type_filter, type_allowed,
                               decoded_count, max_messages,
                               stop_now, verbose);
 
        cursor += (size_t)packet_length;
        packet_count++;
 
        if (stop_now) {
            break;
        }
    }
 
    std::printf(">> INFO: Total Packets=%llu Total Decoded=%llu\n",
                (unsigned long long)packet_count,
                (unsigned long long)decoded_count);
    return 0;
}

int Application::run() {
    if (!load_config(config_path.c_str())) {
        std::printf("Failed to load config: %s\n", config_path.c_str());
        return 1;
    }

    const AppConfig& cfg = config();

    if (!decode_file_path.empty()) {
        return decode_file(decode_file_path, cfg,
                           has_type_filter, type_allowed,
                           max_messages, verbose);
    }

    // Download mode -s <startseq>
    if (has_start_seq) {
        if (cfg.mcast_rerequester_ip.empty() || cfg.mcast_rerequester_port == 0) {
            std::printf("Error: rerequester IP/Port not set in the config\n");
            return 1;
        }

        char session[10];
        std::string session_value;

        if (!get_session_id(cfg, session, session_value)) {
            return 1;
        }

        const int udp_packet_capacity = 64 * 1024;

        // Open rerequester Socket
        // Send request + receive reply
        // packets on same socket.
        Rerequester rr;
        int receive_buffer_bytes = 4 * 1024 * 1024;
        int timeout_ms = 1000;

        if (!rr.open(cfg.mcast_rerequester_ip, cfg.mcast_rerequester_port,
                     receive_buffer_bytes, timeout_ms)) {
            std::printf("Error: failed to open rerequester socket\n");
            return 1;
        }

        uint16_t max_per_request = cfg.max_recovery_message_count;
        if (max_per_request == 0) {
            max_per_request = 5000;
        }

        uint64_t decoded_count = 0;
        uint64_t current_seq = start_seq;

        // If -n is provided => bounded download
        // If -n is not provided => download all
        bool bounded_download = (max_messages != 0);
        uint64_t remaining = max_messages;
        uint64_t total_received = 0;

        uint8_t rxbuf[udp_packet_capacity];

        while (!bounded_download || remaining > 0) {
            uint16_t req_count = max_per_request;
            if (bounded_download) {
                req_count = (remaining > (uint64_t)max_per_request)
                            ? max_per_request
                            : (uint16_t)remaining;
            }

            // Send rereq for current_seq+request_count-1
            if (!rr.send_request(session, current_seq, req_count)) {
                rr.close();
                return 1;
            }

            // Receive enough reply packets to decode this chunk
            uint64_t got_in_chunk = 0;
            int timeouts = 0;

            while (got_in_chunk < (uint64_t)req_count) {
                int n = rr.receive_packet(rxbuf, udp_packet_capacity);
                if (n <= 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        timeouts++;
                        if (timeouts >= 3) {
                            break;
                        }
                        continue;
                    }
                    std::printf("Recovery recv error errno=%d\n", errno);
                    break;
                }

                bool stop_now = false;
                uint16_t processed = decode_packet_messages(rxbuf, n, cfg, has_type_filter, type_allowed,
                                                           decoded_count, max_messages, stop_now, verbose);

                got_in_chunk += (uint64_t)processed;

                if (stop_now) {
                    rr.close();
                    return 0;
                }
            }

            // If nothing arrived
            if (got_in_chunk == 0) {
                rr.close();

                if (total_received > 0) {
                    return 0;
                }

                // if unbound return 0
                // if bounded and didn't get all (-s .. -n ..) exit 1
                if (bounded_download) {
                    return 1;
                }
                return 0;
            }

            total_received += got_in_chunk;
            current_seq += got_in_chunk;

            if (bounded_download) {
                if (got_in_chunk >= remaining) {
                    remaining = 0;
                } else {
                    remaining -= got_in_chunk;
                }
            }
        }

        rr.close();
        return 0;
    }

    // Live Mode
    Socket sock;
    if (!sock.connect_socket(cfg.mcast_ip, cfg.mcast_port, cfg.interface_ip, cfg.mcast_source_ip)) {
        std::printf("Failed to connect socket\n");
        return 1;
    }

    sock.set_receive_buffer(4 * 1024 * 1024);
    const int buffer_capacity = 64 * 1024;
    uint8_t buffer[buffer_capacity];
    std::string current_session;
    bool joined = false;
    uint64_t expected_seq = 0;
    uint64_t decoded_count = 0;

    Rerequester rr;
    bool rr_open = false;

    uint16_t max_per_request = cfg.max_recovery_message_count;
    if (max_per_request == 0) {
        max_per_request = 5000;
    }

    if (enable_recovery) {
        if (cfg.mcast_rerequester_ip.empty() || cfg.mcast_rerequester_port == 0) {
            sock.close();
            return 1;
        }

        int receive_buffer_bytes = 4 * 1024 * 1024;
        int timeout_ms = 1000;

        if (!rr.open(cfg.mcast_rerequester_ip, cfg.mcast_rerequester_port,
                     receive_buffer_bytes, timeout_ms)) {

            std::printf("Error: failed to open rerequester socket\n");
            sock.close();
            return 1;
        }

        rr_open = true;
    }

    while (1) {
        int bytes = sock.receive_bytes(buffer, buffer_capacity);
        if (bytes <= 0) {
            continue;
        }

        MoldHeader header;
        if (!parse_mold_header(buffer, bytes, &header)) {
            continue;
        }

        // End Session 65535
        if ((uint16_t)header.message_count == 0xFFFF) {
            char sess10[11];
            std::memset(sess10, ' ', 10);
            size_t n = header.session.size();
            if (n > 10) n = 10;
            if (n) std::memcpy(sess10, header.session.data(), n);
            sess10[10] = '\0';

            std::printf(">> {'%.*s', %llu, %u}\n",
                        10, sess10,
                        (unsigned long long)header.sequence_number,65535u);
            continue;
        }
 
        if (enable_recovery) {
            check_sequence_gap(header, current_session, joined, expected_seq);
        }

        // Gap-fill
        if (enable_recovery && rr_open && joined &&
            header.session == current_session &&
            header.sequence_number > expected_seq) {

            uint64_t gap_start = expected_seq;
            uint64_t gap_count = header.sequence_number - expected_seq;

            char session[10];
            std::memset(session, ' ', 10);

            size_t session_length = header.session.size();
            if (session_length > 10) {
                session_length = 10;
            }

            std::memcpy(session, header.session.data(), session_length);

            bool gap_stop_now = false;
            gap_fill(rr, session, gap_start, gap_count,
                max_per_request, cfg,
                has_type_filter, type_allowed,
                decoded_count, max_messages,
                gap_stop_now, verbose);

            if (gap_stop_now) {
                rr.close();
                sock.close();
                return 0;
            }
        }

        // Skip Duplicate
        if (joined && header.session == current_session &&
            header.sequence_number < expected_seq) {
            continue;
        }

        bool stop_now = false;
        decode_packet_messages(buffer, bytes, cfg, has_type_filter, type_allowed,
                       decoded_count, max_messages, stop_now, verbose);

        expected_seq = header.sequence_number + (uint64_t)header.message_count;

        if (stop_now) {
            sock.close();
            return 0;
        }
    }

    sock.close();
    return 0;
}
