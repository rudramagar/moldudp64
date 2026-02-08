#include "application.h"
#include "config.h"
#include "socket.h"
#include "decoder.h"
#include "recovery.h"

#include <cstdio>
#include <cstdint>
#include <string>
#include <cstring>
#include <cerrno>

Application::Application()
: max_messages(0),
  verbose(false),
  has_type_filter(false),
  has_start_seq(false),
  start_seq(0),
  enable_recovery(false) {
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
        std::printf(">> INFO: SESSION_CHANGE SequenceNum=%llu\n",
                    (unsigned long long)header.sequence_number);

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

    if (header.sequence_number < expected_seq) {
        std::printf(">> DUPLICATE: ExpectedSequence=%llu Received=%llu, Ignoring...\n",
                    (unsigned long long)expected_seq,
                    (unsigned long long)header.sequence_number);
        return;
    }
}

// Connect to multicast feed, read first valid Mold header
// return session id for:
// -s : get session for rerequest packet
// -g : recovery mode
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

// Purpose:
// Wrapper to decode a full MoldUDP packet (header + payload (all messages))
// Used mode:
// - Live, replay(download) / live + recovery (-g)
//
// Funtions:
// Calls parse_mold_header() to parse Mold header(session, startseq, msgcount)
// Iterates each MoldMessage in the packet
// Computes per-message seq = header.seqnum + msgcount
// Calls decode_itch_message() for each message
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

        // Print filter by message type.
        bool allow_print = true;
        if (has_type_filter) {
            allow_print = type_allowed[(unsigned char)msg[0]];
        }

        if (allow_print) {
            decode_itch_message(msg, msg_len, cfg, header.session, seq, (uint16_t)header.message_count, verbose);
        }

        decoded_count++;
        processed++;

        // Stop after N total messages
        // on -n
        if (max_messages_limit != 0 && decoded_count >= max_messages_limit) {
            stop_now = true;
            return processed;
        }
    }
    return processed;
}

// Gap-fill (Recover)
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
            std::printf("Recovery send_request failed seq=%llu count=%u\n",
                        (unsigned long long)current_seq,
                        (unsigned)req_count);

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
                        if (got_in_chunk == 0) {
                            std::printf("Recovery timeout seq=%llu got=%llu req=%u\n",
                                        (unsigned long long)current_seq,
                                        (unsigned long long)got_in_chunk,
                                        (unsigned)req_count);
                        }
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

int Application::run() {
    const char* config_path = "config/config.ini";
    if (!load_config(config_path)) {
        std::printf("Failed to load config: %s\n", config_path);
        return 1;
    }

    const AppConfig& cfg = config();
    if (verbose) {
        std::printf("verbose on\n");
    }

    // Download mode -s <startseq>
    if (has_start_seq) {
        if (cfg.mcast_rerequester_ip.empty() || cfg.mcast_rerequester_port == 0) {
            std::printf("Error: rerequester IP/Port not set in the config\n");
            return 1;
        }

        // Get valid SessionId from first valid live Mold header
        // To send the rerequest packet to.
        char session[10];
        std::string session_value;

        if (!get_session_id(cfg, session, session_value)) {
            std::printf("Failed to get SessionID\n");
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

        // Request reply
        // in chunks
        // configured in config with 
        // max_recovery_message_count
        uint16_t max_per_request = cfg.max_recovery_message_count;
        if (max_per_request == 0) {
            max_per_request = 5000;
        }

        uint64_t decoded_count = 0;
        uint64_t current_seq = start_seq;

        // If -n is provided => bounded download
        // If -n is not provided => download all (until stalled / Ctrl+C)
        bool bounded_download = (max_messages != 0);
        uint64_t remaining = max_messages;

        uint8_t rxbuf[udp_packet_capacity];

        while (!bounded_download || remaining > 0) {
            uint16_t req_count = max_per_request;
            if (bounded_download) {
                req_count = (remaining > (uint64_t)max_per_request)
                            ? max_per_request
                            : (uint16_t)remaining;
            }

            // Send rerequest for 
            // [current_seq ... current_seq + req_count -1]
            if (!rr.send_request(session, current_seq, req_count)) {
                std::printf(">> ERROR : Recovery Request Send  Failed Sequence=%llu, Count=%u\n",
                            (unsigned long long)current_seq, (unsigned)req_count);
                rr.close();
                return 1;
            }

            std::printf(">> INFO : Requesting... Sequence Number=%llu, Total Message=%u\n",
                        (unsigned long long)current_seq, (unsigned)req_count);

            // Receive reply packets
            // until enough message decode
            // for this chunk
            uint64_t got_in_chunk = 0;
            int timeouts = 0;

            while (got_in_chunk < (uint64_t)req_count) {
                int n = rr.receive_packet(rxbuf, udp_packet_capacity);
                if (n <= 0) {
                    // Timeout is expected sometimes
                    // allow a few then stop this chunk
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        timeouts++;
                        if (timeouts >= 3) {
                            if (got_in_chunk == 0) {
                                std::printf("Recovery timeout seq=%llu got=%llu req=%u\n",
                                            (unsigned long long)current_seq,
                                            (unsigned long long)got_in_chunk,
                                            (unsigned)req_count);
                            }
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
                    std::printf(">> STOP : Total Decoded Messages=%llu\n", (unsigned long long)decoded_count);
                    rr.close();
                    return 0;
                }
            }

            // If nothing arrived
            if (got_in_chunk == 0) {
                std::printf("Recovery stalled seq=%llu req=%u\n",
                            (unsigned long long)current_seq,
                            (unsigned)req_count);

                rr.close();

                // Unbounded (-s without -n): treat stall as end of download (exit OK)
                // Bounded (-s with -n): treat stall as failure (didn't get requested amount)
                return bounded_download ? 1 : 0;
            }

            // Skip broken data 
            // and move forward with 
            // all valid messages (best-effort)
            current_seq += got_in_chunk;

            if (bounded_download) {
                if (got_in_chunk >= remaining) {
                    remaining = 0;
                } else {
                    remaining -= got_in_chunk;
                }
            }
        }

        std::printf("Recovery done decoded_count=%llu\n", (unsigned long long)decoded_count);
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

    // Open rerequester 
    // only if enable_recovery
    Rerequester rr;
    bool rr_open = false;

    uint16_t max_per_request = cfg.max_recovery_message_count;
    if (max_per_request == 0) {
        max_per_request = 5000;
    }

    if (enable_recovery) {
        if (cfg.mcast_rerequester_ip.empty() || cfg.mcast_rerequester_port == 0) {
            std::printf("Error: rerequester IP/Port not set in the config\n");
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
        std::printf("Recovery live mode enabled\n");
    }

    std::printf("Listening... (Ctrl+C to stop)\n");

    while (1) {
        int bytes = sock.receive_bytes(buffer, buffer_capacity);
        if (bytes <= 0) {
            continue;
        }

        MoldHeader header;
        if (!parse_mold_header(buffer, bytes, &header)) {
            continue;
        }

        // Print End-of-seesion message
        // 65535
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

        // Gap/Duplicate/SessionChange
        if (enable_recovery) {
            check_sequence_gap(header, current_session, joined, expected_seq);
        }

        // Gap-fill
        if (enable_recovery && rr_open && joined &&
            header.session == current_session &&
            header.sequence_number > expected_seq) {

            uint64_t gap_start = expected_seq;
            uint64_t gap_count = header.sequence_number - expected_seq;

            // use sessionId from current
            // live packet
            char session[10];
            std::memset(session, ' ', 10);

            size_t session_length = header.session.size();
            if (session_length > 10) {
                session_length = 10;
            }

            std::memcpy(session, header.session.data(), session_length);
            std::printf(">> Start recovering ...\n");

            bool gap_stop_now = false;
            uint64_t recovered_count = gap_fill(rr, session, gap_start, gap_count,
                max_per_request, cfg,
                has_type_filter, type_allowed,
                decoded_count, max_messages,
                gap_stop_now, verbose);

            std::printf(">> RECOVERED: SequenceNumber=%llu, TotalRecovered=%llu\n",
                        (unsigned long long)gap_start,
                        (unsigned long long)recovered_count);

            if (gap_stop_now) {
                std::printf(">> STOP: Total Decoded=%llu\n", (unsigned long long)decoded_count);
                rr.close();
                sock.close();
                return 0;
            }
        }

        // Decode the packet's message
        bool stop_now = false;
        decode_packet_messages(buffer, bytes, cfg, has_type_filter, type_allowed,
                       decoded_count, max_messages, stop_now, verbose);

        // Expected next packet startseq
        expected_seq = header.sequence_number + (uint64_t)header.message_count;

        if (stop_now) {
            std::printf(">> STOP: Total Decoded=%llu\n", (unsigned long long)decoded_count);
            sock.close();
            return 0;
        }
    }

    sock.close();
    return 0;
}
