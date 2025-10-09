#ifndef GPUSPATIAL_DETAILS_RT_ENGINE_HPP
#define GPUSPATIAL_DETAILS_RT_ENGINE_HPP

#include <optix_host.h>
#include <optix_types.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <thrust/device_vector.h>
#include <rmm/cuda_stream.hpp>
#include <rmm/device_uvector.hpp>

#include "gpuspatial/index/detail/sbt_record.h"
#include "gpuspatial/utils/array_view.h"

#define GPUSPATIAL_OPTIX_LAUNCH_PARAMS_NAME "params"

namespace gpuspatial {
namespace details {

#define MODULE_ENABLE_MISS (1 << 0)
#define MODULE_ENABLE_CH (1 << 1)
#define MODULE_ENABLE_AH (1 << 2)
#define MODULE_ENABLE_IS (1 << 3)

class Module {
 public:
  Module() : enabled_module_(0), n_payload_(0), n_attribute_(0) {}

  explicit Module(const std::string& id)
      : id_(id), enabled_module_(0), n_payload_(0), n_attribute_(0) {}

  void EnableMiss() { enabled_module_ |= MODULE_ENABLE_MISS; }
  void EnableClosestHit() { enabled_module_ |= MODULE_ENABLE_CH; }
  void EnableAnyHit() { enabled_module_ |= MODULE_ENABLE_AH; }
  void EnableIsIntersection() { enabled_module_ |= MODULE_ENABLE_IS; }

  bool IsMissEnable() const { return enabled_module_ & MODULE_ENABLE_MISS; }
  bool IsClosestHitEnable() const { return enabled_module_ & MODULE_ENABLE_CH; }
  bool IsAnyHitEnable() const { return enabled_module_ & MODULE_ENABLE_AH; }
  bool IsIsIntersectionEnabled() const { return enabled_module_ & MODULE_ENABLE_IS; }

  void set_id(const std::string& id) { id_ = id; }
  const std::string& get_id() const { return id_; }

  void set_program_path(const std::string& program_path) { program_path_ = program_path; }
  const std::string& get_program_path() const { return program_path_; }

  void set_function_suffix(const std::string& function_suffix) {
    function_suffix_ = function_suffix;
  }
  const std::string& get_function_suffix() const { return function_suffix_; }

  void set_n_payload(int n_payload) { n_payload_ = n_payload; }
  int get_n_payload() const { return n_payload_; }

  void set_n_attribute(int n_attribute) { n_attribute_ = n_attribute; }
  int get_n_attribute() const { return n_attribute_; }

  OptixPipelineCompileOptions get_pipeline_compile_options() const {
    OptixPipelineCompileOptions options;
    options.traversableGraphFlags = OPTIX_TRAVERSABLE_GRAPH_FLAG_ALLOW_SINGLE_GAS;
    options.usesMotionBlur = false;
    options.numPayloadValues = n_payload_;
    options.numAttributeValues = n_attribute_;
    options.exceptionFlags = OPTIX_EXCEPTION_FLAG_NONE;
    options.pipelineLaunchParamsVariableName = GPUSPATIAL_OPTIX_LAUNCH_PARAMS_NAME;
    options.usesPrimitiveTypeFlags = 0;
    options.allowOpacityMicromaps = false;
    return options;
  }

 private:
  std::string id_;
  std::string program_path_;
  std::string function_suffix_;
  int enabled_module_;
  int n_payload_;
  int n_attribute_;
};

struct OptixResources {
  OptixModule module;
  OptixProgramGroup raygen_pg;
  OptixProgramGroup miss_pg;
  OptixProgramGroup hitgroup_pg;
  OptixPipeline pipeline;
  OptixShaderBindingTable sbt;
  thrust::device_vector<RaygenRecord> raygen_records;
  thrust::device_vector<MissRecord> miss_records;
  thrust::device_vector<HitgroupRecord> hitgroup_records;

  OptixResources() = default;
  OptixResources(const OptixResources&) = delete;
  OptixResources& operator=(const OptixResources&) = delete;
};

struct RTConfig {
  RTConfig()
      : max_reg_count(0),
        max_traversable_depth(1),
        max_trace_depth(2),
        logCallbackLevel(1),
        opt_level(OPTIX_COMPILE_OPTIMIZATION_DEFAULT),
        dbg_level(OPTIX_COMPILE_DEBUG_LEVEL_NONE),
        n_pipelines(1) {}

  void AddModule(const Module& mod);

  int max_reg_count;
  int max_traversable_depth;
  int max_trace_depth;
  int logCallbackLevel;
  OptixCompileOptimizationLevel opt_level;
  OptixCompileDebugLevel dbg_level;
  std::map<std::string, Module> modules;
  int n_pipelines;
};

RTConfig get_default_rt_config(const std::string& ptx_root);

class RTEngine {
 public:
  RTEngine();
  ~RTEngine();

  void Init(const RTConfig& config);

  OptixTraversableHandle BuildAccelCustom(cudaStream_t cuda_stream,
                                          ArrayView<OptixAabb> aabbs,
                                          rmm::device_uvector<char>& out_buf,
                                          bool prefer_fast_build = false,
                                          bool compact = false);

  void Render(cudaStream_t cuda_stream, const std::string& id, dim3 dim,
              const ArrayView<char>& params);

  OptixDeviceContext get_context() const;

  size_t EstimateMemoryUsageForAABB(size_t num_aabbs, bool prefer_fast_build,
                                    bool compact);

 private:
  void initOptix(const RTConfig& config);
  void createContext();
  void createModule(const RTConfig& config);
  void createRaygenPrograms(const RTConfig& config);
  void createMissPrograms(const RTConfig& config);
  void createHitgroupPrograms(const RTConfig& config);
  void createPipeline(const RTConfig& config);
  void buildSBT(const RTConfig& config);
  void releaseOptixResources();

  static size_t getAccelAlignedSize(size_t size);
  static std::vector<char> readData(const std::string& filename);

  CUcontext cuda_context_;
  OptixDeviceContext optix_context_;
  OptixModuleCompileOptions module_compile_options_ = {};
  OptixPipelineLinkOptions pipeline_link_options_ = {};
  std::map<std::string, OptixResources> resources_;
  bool initialized_;
};

}  // namespace details
}  // namespace gpuspatial

#endif  // GPUSPATIAL_DETAILS_RT_ENGINE_HPP