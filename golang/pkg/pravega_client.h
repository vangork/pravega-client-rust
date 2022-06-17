/* Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved. */

#ifndef PRAVEGA_CLIENT_H
#define PRAVEGA_CLIENT_H

/* Generated with cbindgen:0.23.0 */

/* Warning, this file is autogenerated by cbindgen. Don't modify this manually. */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum CredentialsType {
  Basic = 0,
  BasicWithToken = 1,
  Keycloak = 2,
  KeycloakFromJsonString = 3,
} CredentialsType;

typedef enum RetentionTypeMapping {
  None = 0,
  Time = 1,
  Size = 2,
} RetentionTypeMapping;

typedef enum ScaleTypeMapping {
  FixedNumSegments = 0,
  ByRateInKbytesPerSec = 1,
  ByRateInEventsPerSec = 2,
} ScaleTypeMapping;

typedef struct Slice Slice;

typedef struct StreamManager StreamManager;

typedef struct StreamReader StreamReader;

typedef struct StreamReaderGroup StreamReaderGroup;

typedef struct StreamScalingPolicy StreamScalingPolicy;

typedef struct StreamWriter StreamWriter;

typedef struct Buffer {
  uint8_t *ptr;
  uintptr_t len;
  uintptr_t cap;
} Buffer;

typedef struct RetryWithBackoffMapping {
  uint64_t initial_delay;
  uint32_t backoff_coefficient;
  uint64_t max_delay;
  int32_t max_attempt;
  int64_t expiration_time;
} RetryWithBackoffMapping;

typedef struct CredentialsMapping {
  enum CredentialsType credential_type;
  const char *username;
  const char *password;
  const char *token;
  const char *path;
  const char *json;
  bool disable_cert_verification;
} CredentialsMapping;

typedef struct ClientConfigMapping {
  uint32_t max_connections_in_pool;
  uintptr_t max_controller_connections;
  struct RetryWithBackoffMapping retry_policy;
  const char *controller_uri;
  uintptr_t transaction_timeout_time;
  bool is_tls_enabled;
  bool disable_cert_verification;
  const char *trustcerts;
  struct CredentialsMapping credentials;
  bool is_auth_enabled;
  uintptr_t reader_wrapper_buffer_size;
  uintptr_t request_timeout;
} ClientConfigMapping;

typedef struct ScalingMapping {
  enum ScaleTypeMapping scale_type;
  int32_t target_rate;
  int32_t scale_factor;
  int32_t min_num_segments;
} ScalingMapping;

typedef struct RetentionMapping {
  enum RetentionTypeMapping retention_type;
  int64_t retention_param;
} RetentionMapping;

typedef struct StreamConfigurationMapping {
  const char *scope;
  const char *stream;
  struct ScalingMapping scaling;
  struct RetentionMapping retention;
  const char *tags;
} StreamConfigurationMapping;

void free_buffer(struct Buffer buf);

extern void publishBridge(int64_t chan_id, uintptr_t obj_ptr);

struct StreamManager *stream_manager_new(struct ClientConfigMapping client_config,
                                         struct Buffer *err);

void stream_manager_destroy(struct StreamManager *manager);

bool stream_manager_create_scope(const struct StreamManager *manager,
                                 const char *scope,
                                 struct Buffer *err);

bool stream_manager_create_stream(const struct StreamManager *manager,
                                  struct StreamConfigurationMapping stream_config,
                                  struct Buffer *err);

struct StreamWriter *stream_writer_new(const struct StreamManager *manager,
                                       const char *scope,
                                       const char *stream,
                                       uintptr_t max_inflight_events,
                                       struct Buffer *err);

void stream_writer_destroy(struct StreamWriter *writer);

struct StreamReaderGroup *stream_reader_group_new(const struct StreamManager *manager,
                                                  const char *reader_group,
                                                  const char *scope,
                                                  const char *stream,
                                                  bool read_from_tail,
                                                  struct Buffer *err);

void stream_reader_group_destroy(struct StreamReaderGroup *rg);

struct StreamScalingPolicy *fixed_scaling_policy(int32_t num);

void scaling_policy_destroy(struct StreamScalingPolicy *policy);

void stream_writer_write_event(struct StreamWriter *writer,
                               struct Buffer event,
                               struct Buffer routing_key,
                               struct Buffer *err);

void stream_writer_flush(struct StreamWriter *writer, struct Buffer *err);

struct StreamReader *stream_reader_group_create_reader(const struct StreamReaderGroup *reader_group,
                                                       const char *reader,
                                                       struct Buffer *err);

void stream_reader_destroy(struct StreamReader *reader);

void stream_reader_get_segment_slice(struct StreamReader *reader,
                                     int64_t chan_id,
                                     struct Buffer *err);

void segment_slice_destroy(struct Slice *slice);

void stream_reader_release_segment_slice(struct StreamReader *reader,
                                         struct Slice *slice,
                                         struct Buffer *err);

void segment_slice_next(struct Slice *slice, struct Buffer *event, struct Buffer *err);

#endif /* PRAVEGA_CLIENT_H */
