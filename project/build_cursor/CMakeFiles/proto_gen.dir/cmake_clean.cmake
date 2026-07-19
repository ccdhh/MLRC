file(REMOVE_RECURSE
  "../src/proto/coordinator.grpc.pb.cc"
  "../src/proto/coordinator.grpc.pb.h"
  "../src/proto/coordinator.pb.cc"
  "../src/proto/coordinator.pb.h"
  "../src/proto/datanode.grpc.pb.cc"
  "../src/proto/datanode.grpc.pb.h"
  "../src/proto/datanode.pb.cc"
  "../src/proto/datanode.pb.h"
  "../src/proto/proxy.grpc.pb.cc"
  "../src/proto/proxy.grpc.pb.h"
  "../src/proto/proxy.pb.cc"
  "../src/proto/proxy.pb.h"
  "CMakeFiles/proto_gen"
  "proto_gen.stamp"
)

# Per-language clean rules from dependency scanning.
foreach(lang )
  include(CMakeFiles/proto_gen.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
