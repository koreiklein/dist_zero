import random
import os
import time

import capnp

import matplotlib.pyplot as plt
import numpy as np

from dist_zero import cgen


def test_pass_buffer_c_to_python():
  program = cgen.Program(name='simple_buffer_perf_test')

  globalBuf = program.AddDeclaration(cgen.Char.Star().Var('global_buffer'))

  nGlobalBufBytes = 10 * 1000 * 1000

  initGlobalBuf = program.AddExternalFunction(name='InitGlobalBuf', args=None)
  initGlobalBuf.AddAssignment(globalBuf, cgen.malloc(cgen.Constant(nGlobalBufBytes)).Cast(cgen.Char.Star()))

  index = initGlobalBuf.AddDeclaration(cgen.MachineInt.Var('index'), cgen.Zero)
  loop = initGlobalBuf.AddWhile(index < cgen.Constant(nGlobalBufBytes))

  loop.AddAssignment(globalBuf.Sub(index), cgen.Constant(120))

  loop.AddAssignment(index, index + cgen.One)

  initGlobalBuf.AddReturn(cgen.PyBool_FromLong(cgen.Constant(1)))

  vSize = cgen.Int32.Var('size')
  f = program.AddExternalFunction(name='F', args=[vSize])

  f.AddReturn(cgen.PyBytes_FromStringAndSize(globalBuf, vSize))
  #f.AddReturn(cgen.PyMemoryView_FromMemory(globalBuf, vSize, cgen.Zero))

  mod = program.build_and_import()

  # Initialize the global buffer
  mod.InitGlobalBuf()

  def test_i(i):
    n_samples = 200
    start = time.time()
    for x in range(n_samples):
      start = time.time()
      bs = mod.F(i)
      if len(bs) != i:
        raise RuntimeError("Bad length")
      if bs[2:3] != b'x':
        raise RuntimeError("Bad char")
    duration = time.time() - start
    return duration / n_samples

  xs, ys = [], []
  for i in range(100, 5000 * 1000, 10000):
    print(f"testing {i}")
    xs.append(i / 1000000)
    ys.append(test_i(i) * 1000000)

  fig, ax = plt.subplots()

  ax.plot(xs, ys)
  ax.set(xlabel='size of buffer (megabytes)', ylabel='time to pass and receive from c extension (microseconds)')
  plt.show()


def test_pass_buffer_python_to_c():
  program = cgen.Program(name='simple_buffer_perf_test')
  vArgs = cgen.PyObject.Star().Var('args')
  f = program.AddExternalFunction(name='F', args=None)

  vBuf = f.AddDeclaration(cgen.UInt8.Star().Var('buf'))
  vBuflen = f.AddDeclaration(cgen.MachineInt.Var('buflen'))

  whenParseFail = f.AddIf(
      cgen.PyArg_ParseTuple(vArgs, cgen.StrConstant("s#"), vBuf.Address(), vBuflen.Address()).Negate()).consequent
  whenParseFail.AddReturn(cgen.PyLong_FromLong(cgen.Constant(0)))

  resultBuf = cgen.PyObject.Star().Var('result')
  f.AddReturn(cgen.PyLong_FromLong(vBuflen))

  mod = program.build_and_import()

  buffers = [''.join('x' for x in range(i * 7000)).encode('utf-8') for i in range(10, 5000, 180)]

  def test_buf(buf):
    n_samples = 200
    start = time.time()
    for i in range(n_samples):
      flen = mod.F(buf)
      if len(buf) != flen:
        raise RuntimeError("Bad length")
    duration = time.time() - start

    return duration / n_samples

  fig, ax = plt.subplots()

  xs, ys = [], []

  print('running tests')
  for buf in buffers:
    buflen = len(buf) / 1000000
    buftime = 1000000 * test_buf(buf)
    xs.append(buflen)
    ys.append(buftime)
    print(f'finished test of size {buflen}')

  ax.plot(xs, ys)
  ax.set(xlabel='size of buffer (megabytes)', ylabel='time to pass and receive from c extension (microseconds)')
  plt.show()


if __name__ == '__main__':
  #test_pass_buffer_python_to_c()
  test_pass_buffer_c_to_python()
