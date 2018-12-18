#include "kvec.h"
#include <Python.h>

enum Three_enum {
  Three_enum_option_field0 = 0,
  Three_enum_option_field1 = 1,
  Three_enum_option_field2 = 2,
};

enum Two_enum {
  Two_enum_option_field0 = 0,
  Two_enum_option_field1 = 1,
};

enum ProductmbGRli5Transition_enum {
  ProductmbGRli5Transition_enum_option_jump = 0,
  ProductmbGRli5Transition_enum_option_simultaneous = 1,
};

enum ThreeTransition_enum {
  ThreeTransition_enum_option_sum_on_field0 = 0,
  ThreeTransition_enum_option_sum_on_field1 = 1,
  ThreeTransition_enum_option_sum_on_field2 = 2,
};

enum UnitTransition_enum {
  UnitTransition_enum_option_simultaneous = 0,
};

struct ProductmbGRli5;
struct Three;
struct Unit;
struct Two;
struct ProductmbGRli5Transition;
struct ProductmbGRli5_simultaneous;
struct ThreeTransition;
struct UnitTransition;
struct Unit_simultaneous;
union Three_union;
union Two_union;
union ProductmbGRli5Transition_union;
union ThreeTransition_union;
union UnitTransition_union;
union Three_union {
  struct Unit *(field0);
  struct Unit *(field1);
  struct Unit *(field2);
};

union Two_union {
  struct Unit *(field0);
  struct Unit *(field1);
};

union ProductmbGRli5Transition_union {
  struct ProductmbGRli5 *(jump);
  struct ProductmbGRli5_simultaneous *(simultaneous);
};

union ThreeTransition_union {
  struct UnitTransition *(sum_on_field0);
  struct UnitTransition *(sum_on_field1);
  struct UnitTransition *(sum_on_field2);
};

union UnitTransition_union {
  struct Unit_simultaneous *(simultaneous);
};

struct ProductmbGRli5 {
  struct Three *(left);
  kvec_t(struct Two *) *(right);
};

struct Three {
  enum Three_enum type;
  union Three_union value;
};

struct Unit {
};

struct Two {
  enum Two_enum type;
  union Two_union value;
};

struct ProductmbGRli5Transition {
};

struct ProductmbGRli5_simultaneous {
  struct ThreeTransition *(left);
  void *(right);
};

struct ThreeTransition {
};

struct UnitTransition {
};

struct Unit_simultaneous {
};


static struct PyMethodDef test_program_Methods[] = {
  {NULL, NULL, 0, NULL},
};

static struct PyModuleDef test_program_Module = {
  PyModuleDef_HEAD_INIT,
  "test_program",
  "",
  -1,
  test_program_Methods
};

PyMODINIT_FUNC
PyInit_test_program(void) {
  return PyModule_Create(&test_program_Module);
}

