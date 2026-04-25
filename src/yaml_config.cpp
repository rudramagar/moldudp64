#include "yaml_config.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

typedef std::map<std::string, std::string> FlatMap;

static std::string strip(const std::string& input) {
    size_t begin = 0;
    size_t end   = input.size();

    while (begin < end) {
        char ch = input[begin];
        if (ch != ' ' && ch != '\t' && ch != '\r') {
            break;
        }
        ++begin;
    }
    while (end > begin) {
        char ch = input[end - 1];
        if (ch != ' ' && ch != '\t' && ch != '\r') {
            break;
        }
        --end;
    }

    std::string trimmed = input.substr(begin, end - begin);

    // Strip matching outer quotes.
    if (trimmed.size() >= 2) {
        char first = trimmed[0];
        char last  = trimmed[trimmed.size() - 1];
        if ((first == '"' || first == '\'') && first == last) {
            trimmed = trimmed.substr(1, trimmed.size() - 2);
        }
    }
    return trimmed;
}

static bool read_yaml(const char* path, FlatMap* out) {
    std::ifstream file(path);
    if (!file) {
        return false;
    }

    std::vector<int>         indent_stack;
    std::vector<std::string> key_stack;
    std::string              line;

    while (std::getline(file, line)) {
        // Strip "#" comment.
        size_t hash_pos = line.find('#');
        if (hash_pos != std::string::npos) {
            line.resize(hash_pos);
        }

        // Count leading spaces.
        int indent = 0;
        while (indent < (int)line.size() && line[indent] == ' ') {
            ++indent;
        }
        // Skip blank line
        if (indent == (int)line.size()) {
            continue;
        }

        // Find the "key: value" colon.
        size_t colon_pos = line.find(':', indent);
        if (colon_pos == std::string::npos) {
            continue;
        }

        std::string key   = strip(line.substr(indent, colon_pos - indent));
        std::string value = strip(line.substr(colon_pos + 1));

        // Pop any sections at this indent or deeper.
        while (!indent_stack.empty() && indent_stack.back() >= indent) {
            indent_stack.pop_back();
            key_stack.pop_back();
        }

        if (value.empty()) {
            // Section header — push as a new open level.
            indent_stack.push_back(indent);
            key_stack.push_back(key);
            continue;
        }

        // Leaf — build the dotted path and store.
        std::string dotted_path;
        for (size_t i = 0; i < key_stack.size(); ++i) {
            dotted_path += key_stack[i];
            dotted_path += '.';
        }
        dotted_path += key;
        (*out)[dotted_path] = value;
    }
    return true;
}

// Spec loader
static FieldType parse_field_type(const std::string& type_name) {
    if (type_name == "char")   return CHAR;
    if (type_name == "uint8")  return UINT8;
    if (type_name == "uint16") return UINT16;
    if (type_name == "uint32") return UINT32;
    if (type_name == "uint64") return UINT64;
    if (type_name == "int16")  return INT16;
    if (type_name == "int32")  return INT32;
    if (type_name == "int64")  return INT64;
    if (type_name == "string") return STRING;
    if (type_name == "binary") return BINARY;
    return STRING;
}

static bool load_spec(const std::string& spec_path, AppConfig* cfg) {
    std::ifstream file(spec_path.c_str());
    if (!file) {
        return false;
    }

    json root;
    file >> root;

    cfg->msg_specs.clear();
    std::memset(cfg->spec_by_type, 0, sizeof(cfg->spec_by_type));

    for (json::iterator it = root.begin(); it != root.end(); ++it) {
        if (it.key().size() != 1) {
            continue;
        }
        const json& msg_obj = it.value();

        MsgSpec msg;
        msg.msg_type = it.key()[0];
        msg.name     = msg_obj.value("name", "");

        uint32_t offset = 0;
        const json& field_array = msg_obj["fields"];
        for (size_t i = 0; i < field_array.size(); ++i) {
            FieldSpec field;
            field.name   = field_array[i].value("name", "");
            field.type   = parse_field_type(field_array[i].value("type", "string"));
            field.size   = (uint32_t)field_array[i].value("size", 0);
            field.offset = offset;

            offset += field.size;
            msg.fields.push_back(field);
        }
        msg.total_length = offset;

        cfg->msg_specs[msg.msg_type] = msg;
        cfg->spec_by_type[(unsigned char)msg.msg_type] = &cfg->msg_specs[msg.msg_type];
    }
    return true;
}

// Public API
AppConfig::AppConfig()
    : next_sequence_number(0),
      mcast_port(0),
      mcast_rerequester_port(0),
      max_recovery_message_count(5000) {
    std::memset(spec_by_type, 0, sizeof(spec_by_type));
}

static AppConfig app_config;

// Resolve config/spec path
static std::string resolve_path(const char* config_path,
                                const std::string& relative_path) {
    if (relative_path.empty()) {
        return relative_path;
    }
    if (relative_path[0] == '/') {
        return relative_path;
    }

    std::string config_path_str = config_path;
    size_t last_slash = config_path_str.find_last_of('/');
    if (last_slash == std::string::npos) {
        return relative_path;
    }
    return config_path_str.substr(0, last_slash) + "/" + relative_path;
}

bool load_config(const char* config_path) {
    FlatMap settings;
    if (!read_yaml(config_path, &settings)) {
        std::fprintf(stderr, "config: cannot open %s\n", config_path);
        return false;
    }

    AppConfig cfg;

    std::string spec_value              = settings["itch_udp_settings.spec"];
    std::string seq_value               = settings["itch_udp_settings.realtime.next_sequence_number"];
    std::string mcast_port_value        = settings["itch_udp_settings.realtime.multicast_port"];
    std::string rerequest_port_value    = settings["itch_udp_settings.recovery.rerequester_port"];
    std::string max_recovery_value      = settings["itch_udp_settings.recovery.max_recovery_message_count"];

    cfg.protocol_spec                   = resolve_path(config_path, spec_value);
    cfg.next_sequence_number            = std::strtoull(seq_value.c_str(), 0, 10);
    cfg.mcast_ip                        = settings["itch_udp_settings.realtime.multicast_address"];
    cfg.mcast_port                      = (uint16_t)std::strtoul(mcast_port_value.c_str(), 0, 10);
    cfg.interface_ip                    = settings["itch_udp_settings.realtime.interface"];
    cfg.mcast_source_ip                 = settings["itch_udp_settings.realtime.source_address"];
    cfg.mcast_rerequester_ip            = settings["itch_udp_settings.recovery.rerequester_address"];
    cfg.mcast_rerequester_port          = (uint16_t)std::strtoul(rerequest_port_value.c_str(), 0, 10);
    cfg.max_recovery_message_count      = (uint16_t)std::strtoul(max_recovery_value.c_str(), 0, 10);

    if (cfg.max_recovery_message_count == 0) {
        cfg.max_recovery_message_count = 5000;
    }

    if (cfg.mcast_ip.empty()) {
        std::fprintf(stderr, "ERROR: missing multicast_address\n");
        return false;
    }
    if (cfg.mcast_port == 0) {
        std::fprintf(stderr, "ERROR: missing multicast_port\n");
        return false;
    }
    if (cfg.interface_ip.empty()) {
        std::fprintf(stderr, "ERROR: missing interface\n");
        return false;
    }
    if (cfg.protocol_spec.empty()) {
        std::fprintf(stderr, "ERROR: missing spec\n");
        return false;
    }

    app_config = cfg;
    if (!load_spec(app_config.protocol_spec, &app_config)) {
        std::fprintf(stderr, "ERROR: cannot load spec %s\n",
                     app_config.protocol_spec.c_str());
        return false;
    }
    return true;
}

const AppConfig& config() {
    return app_config;
}
