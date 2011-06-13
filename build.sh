# build rebar(erlang release handling tool)
mkdir tools
cd tools
git clone git://github.com/basho/rebar.git
cd rebar
./bootstrap
cd ..
cd ..

# compile erlang code
./tools/rebar/rebar compile
# package release
./tools/rebar/rebar generate