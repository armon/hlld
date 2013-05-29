import platform
plat = platform.system()

envmurmur = Environment(CPPPATH = ['deps/murmurhash/'], CPPFLAGS="-fno-exceptions -O2")
murmur = envmurmur.Library('murmur', Glob("deps/murmurhash/*.cpp"))

envinih = Environment(CPATH = ['deps/inih/'], CFLAGS="-O2")
inih = envinih.Library('inih', Glob("deps/inih/*.c"))

env_with_err = Environment(CCFLAGS = '-g -std=c99 -D_GNU_SOURCE -Wall -Wextra -Werror -O2 -pthread -Isrc/ -Ideps/inih/ -Ideps/libev/')
env_without_unused_err = Environment(CCFLAGS = '-g -std=c99 -D_GNU_SOURCE -Wall -Wextra -Wno-unused-function -Wno-unused-result -Werror -O2 -pthread -Isrc/ -Ideps/inih/ -Ideps/libev/')
env_without_err = Environment(CCFLAGS = '-g -std=c99 -D_GNU_SOURCE -O2 -pthread -Isrc/ -Ideps/inih/ -Ideps/libev/')

objs =  env_with_err.Object('src/config', 'src/config.c') + \
        env_with_err.Object('src/barrier', 'src/barrier.c') + \
        env_with_err.Object('src/hll', 'src/hll.c') + \
        env_with_err.Object('src/hll_constants', 'src/hll_constants.c') + \
        env_with_err.Object('src/bitmap', 'src/bitmap.c') + \
        env_with_err.Object('src/set', 'src/set.c') + \
        env_with_err.Object('src/set_manager', 'src/set_manager.c') + \
        env_without_err.Object('src/networking', 'src/networking.c') + \
        env_with_err.Object('src/conn_handler', 'src/conn_handler.c') + \
        env_with_err.Object('src/background', 'src/background.c') + \
        env_with_err.Object('src/art', 'src/art.c')

libs = ["pthread", murmur, inih, "m"]
if plat == 'Linux':
   libs.append("rt")

hlld = env_with_err.Program('hlld', objs + ["src/hlld.c"], LIBS=libs)

if plat == "Darwin":
    test = env_without_err.Program('test_runner', objs + Glob("tests/runner.c"), LIBS=libs + ["check"])
else:
    test = env_without_unused_err.Program('test_runner', objs + Glob("tests/runner.c"), LIBS=libs + ["check"])

bench_obj = Object("bench", "bench.c", CCFLAGS="-std=c99 -O2")
Program('bench', bench_obj, LIBS=["pthread"])

# By default, only compile hlld
Default(hlld)
