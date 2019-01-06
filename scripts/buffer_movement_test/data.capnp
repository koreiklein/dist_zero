@0xf6ae89df702119f9;

using C = import "/c-capnproto/compiler/c.capnp";
$C.fieldgetset;

struct MainStruct {
  values @0 :List(Int32);
}
