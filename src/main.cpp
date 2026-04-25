#include "application.h"
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <stdint.h>
#include <unistd.h>
#include <getopt.h>

static void usage(const char* prog) {
    std::fprintf(stderr,
            "Usage: %s [-g] [-s <seq>] [-n <count>] [-v] [--type <X> ...]\n\n"
            "Options:\n"
            "   -c <config>     custom config path (default: config/config.yaml)\n"
            "   -d <rawfile>    decode MoldUDP packet from file\n"
            "   -g              gap-fill mode\n"
            "   -s <seq>        get data starting at <seq>\n"
            "   -n <count>      stops after decoding <count> msg\n"
            "   -v              verbose mode\n"
            "   --type <X>      filter message type <X> (repeatable)\n"
            "   -h              show help\n",
            prog);
}

int main(int argc, char** argv) {
    // Make stdout/stderr
    // buffered with piped
    std::setvbuf(stdout, 0, _IOLBF, 0);
    std::setvbuf(stderr, 0, _IOLBF, 0);

    uint64_t max_messages = 0;
    bool verbose = false;
    bool enable_recovery = false;
    uint64_t start_seq = 0;
    bool has_start_seq = false;

    Application app;

    static struct option long_options[] = {
        {"type", required_argument, 0, 1000},
        {0, 0, 0, 0}
    };

    int opt;
    int long_index = 0;

    while ((opt = getopt_long(argc, argv, "c:d:gs:n:vh", long_options, &long_index)) != -1) {
        if (opt == 1000) {
            // --type
            if (!optarg || std::strlen(optarg) != 1) {
                std::fprintf(stderr, "Invalid --type value (expect single char): %s\n",
                        optarg ? optarg : "(null)");
                usage(argv[0]);
                return 1;
            }
            app.set_type_filter(optarg[0]);
            continue;
        }

        switch (opt) {
            case 'c':
                app.set_config_path(optarg);
                break;

            case 'd':
                app.set_decode_file(optarg);
                break;

            case 'g':
                enable_recovery = true;
                break;

            case 's': {
                char* end = 0;
                unsigned long long v = std::strtoull(optarg, &end, 10);
                if (end == optarg || *end != '\0') {
                    std::fprintf(stderr, "Invalid -s value: %s\n", optarg);
                    usage(argv[0]);
                    return 1;
                }
                start_seq = (uint64_t)v;
                has_start_seq = true;
                break;
            }
                
            case 'n': {
                char* end = 0;
                unsigned long long v = std::strtoull(optarg, &end, 10);
                if (end == optarg || *end != '\0') {
                    std::fprintf(stderr, "Invalid -n value: %s\n", optarg);
                    usage(argv[0]);
                    return 1;
                }
                max_messages = (uint64_t)v;
                break;
            }

            case 'v':
                verbose = true;
                break;

            case 'h':
                usage(argv[0]);
                return 0;

            default:
                usage(argv[0]);
                return 1;
        }
    }

    if (has_start_seq) {
        app.set_start_seq(start_seq);
    }

    app.set_enable_recovery(enable_recovery);
    app.set_max_messages(max_messages);
    app.set_verbose(verbose);

    return app.run();
}
