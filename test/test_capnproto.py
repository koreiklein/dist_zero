import os

import pytest

from dist_zero import capnpgen, errors


def test_capnproto_error():
  f = capnpgen.CapnpFile(capnpgen.gen_capn_uid())

  struct = f.AddStructure('XX')
  union = struct.AddUnion()

  dirname = os.path.join('.tmp', 'capnp')
  os.makedirs(dirname, exist_ok=True)
  with pytest.raises(errors.CapnpCompileError):
    f.build_in(dirname=dirname, filename='example.capnp')

  struct.AddUnion("HasName")
  struct.AddUnion("HasOtherName")

  with pytest.raises(errors.CapnpFormatError):
    struct.AddUnion()


def test_capnproto_generator():
  f = capnpgen.CapnpFile(capnpgen.gen_capn_uid())

  date = f.AddStructure('Date')
  date.AddField('year', capnpgen.Int16)
  date.AddField('month', capnpgen.UInt8)
  date.AddField('day', capnpgen.UInt8)

  person = f.AddStructure('Person')
  person.AddField('name', capnpgen.Text)
  person.AddField('bithday', date)

  employment = person.AddUnion('employment')
  employment.AddField('unemployed', capnpgen.Void)
  employment.AddField('selfEmployed', capnpgen.Void)

  dirname = os.path.join('.tmp', 'capnp')
  os.makedirs(dirname, exist_ok=True)
  f.build_in(dirname=dirname, filename='example.capnp')
