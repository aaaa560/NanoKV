#include "parser.hpp"

namespace py = pybind11;


int add(int num1, int num2) {
    return num1 + num2;
}

cstr normalize_bool(cstr s) {
    std::transform(
        s.begin(), s.end(),
        s.begin(),
        [](unsigned char c) { return std::tolower(c); }
    );
    return s;
}

cstr replace(cstr s, char toReplace, char replaceWith) {
    std::replace(s.begin(), s.end(), toReplace, replaceWith);
    return s;
}

svector split(const cstr &s, char sep) {
    svector out;
    sstr ss(s);
    cstr item;

    while (std::getline(ss, item, sep)) {
        out.push_back(item);
    }

    return out;
}

svector tsplit(const cstr &s, char sep1, char sep2) {
    svector result;
    result.reserve(4);

    cstr current;
    current.reserve(s.size());

    bool inString = false;

    for (char c: s) {
        if (c == '"') {
            inString = !inString;
        }

        if (c == sep1 || c == sep2) {
            if (inString) {
                current += c;
            } else if (!current.empty()) {
                result.push_back(current);
                current.clear();
            }
        } else {
            current += c;
        }
    }

    if (!current.empty()) {
        result.push_back(current);
    }

    return result;
}

bool in_line(cstr &line, char it) {
    for (char c: line) {
        if (it == c) {
            return true;
        }
    }
    return false;
}

data_type parse(const cstr &file, char sep = '|') {
    data_type result;

    cstr line;
    line.reserve(256);

    std::ifstream inputFile(file);

    if (!inputFile.is_open()) {
        throw std::runtime_error("Erro ao abrir o arquivo");
    }


    while (std::getline(inputFile, line)) {
        line.erase(0, line.find_first_not_of(" \t\n\r"));
        line.erase(line.find_last_not_of(" \t\n\r") + 1);

        if (line.empty() || line[0] == '#') {
            continue;
        }

        if (in_line(line, '#')) {
            line = split(line, '#')[0];
        }

        if (line.ends_with(' ')) {
            line = split(line, ' ').back();
        }

        auto parts = tsplit(line, sep, ':');

        if (parts.size() < 3) {
            continue;
        }

        auto key = parts[0];
        auto type = parts[1];
        auto val = parts[2];

        if (val[0] == '\"' && val[val.length() - 1] == '\"') {
            val = val.substr(1, val.length() - 2);
        }

        if (type == "int") {
            try {
                auto parsed = py::int_(std::stoi(val));
                result[py::str(key)] = parsed;
            } catch (std::invalid_argument e) {
                result[py::str(key)] = val;
            }
        } else if (type == "float") {
            try {
                auto parsed = py::float_(std::stof(val));
                result[py::str(key)] = parsed;
            } catch (std::invalid_argument e) {
                result[py::str(key)] = val;
            }
        } else if (type == "bool") {
            if (normalize_bool(val) == "true") {
                result[py::str(key)] = py::bool_(true);
            } else if (normalize_bool(val) == "false") {
                result[py::str(key)] = py::bool_(false);
            } else {
                result[py::str(key)] = val;
            }
        } else if (type == "str") {
            val.erase(std::remove(val.begin(), val.end(), '\"'), val.end());
            result[py::str(key)] = py::str(val);
        }
    }

    inputFile.close();

    return result;
}

PYBIND11_MODULE(nkv_parser, m) {
    m.doc() = "Um teste do PyBind11";

    m.def("add", &add, "Uma função que adiciona dois números");
    m.def("parse", &parse, "A função pra ler o arquivo NKV", py::arg("file"), py::arg("sep") = '|');
    m.def("tsplit", &tsplit, "Função que divide uma string em dois separadores", py::arg("s"), py::arg("sep1"),
          py::arg("sep2"));
    m.def("split", &split, "Função que separa a string em um ponto", py::arg("string"), py::arg("sep"));
}
