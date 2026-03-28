#pragma once

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#define cstr std::string
#define data_type pybind11::dict
#define svector std::vector<cstr>
#define sstr std::stringstream