/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2024 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_http_client.h>
#include <fluent-bit/flb_oauth2.h>
#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <fluent-bit/flb_scheduler.h>
#include <fluent-bit/flb_gzip.h>
#include <fluent-bit/flb_utils.h>
#include <stdio.h>
#include <fluent-bit/flb_sds.h>
#include <fluent-bit/flb_fstore.h>
#include <msgpack.h>
#include <cJSON.h>

#include "azure_kusto.h"
#include "azure_kusto_conf.h"
#include "azure_kusto_ingest.h"
#include "azure_kusto_store.h"

/* Create a new oauth2 context and get a oauth2 token */
static int azure_kusto_get_oauth2_token(struct flb_azure_kusto *ctx)
{
    int ret;
    char *token;

    /* Clear any previous oauth2 payload content */
    flb_oauth2_payload_clear(ctx->o);

    ret = flb_oauth2_payload_append(ctx->o, "grant_type", 10, "client_credentials", 18);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "error appending oauth2 params");
        return -1;
    }

    ret = flb_oauth2_payload_append(ctx->o, "scope", 5, FLB_AZURE_KUSTO_SCOPE, 39);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "error appending oauth2 params");
        return -1;
    }

    ret = flb_oauth2_payload_append(ctx->o, "client_id", 9, ctx->client_id, -1);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "error appending oauth2 params");
        return -1;
    }

    ret = flb_oauth2_payload_append(ctx->o, "client_secret", 13, ctx->client_secret, -1);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "error appending oauth2 params");
        return -1;
    }

    /* Retrieve access token */
    token = flb_oauth2_token_get(ctx->o);
    if (!token) {
        flb_plg_error(ctx->ins, "error retrieving oauth2 access token");
        return -1;
    }

    return 0;
}

flb_sds_t get_azure_kusto_token(struct flb_azure_kusto *ctx)
{
    int ret = 0;
    flb_sds_t output = NULL;

    if (pthread_mutex_lock(&ctx->token_mutex)) {
        flb_plg_error(ctx->ins, "error locking mutex");
        return NULL;
    }

    if (flb_oauth2_token_expired(ctx->o) == FLB_TRUE) {
        ret = azure_kusto_get_oauth2_token(ctx);
    }

    /* Copy string to prevent race conditions (get_oauth2 can free the string) */
    if (ret == 0) {
        output = flb_sds_create_size(flb_sds_len(ctx->o->token_type) +
                                     flb_sds_len(ctx->o->access_token) + 2);
        if (!output) {
            flb_plg_error(ctx->ins, "error creating token buffer");
            return NULL;
        }
        flb_sds_snprintf(&output, flb_sds_alloc(output), "%s %s", ctx->o->token_type,
                         ctx->o->access_token);
    }

    if (pthread_mutex_unlock(&ctx->token_mutex)) {
        flb_plg_error(ctx->ins, "error unlocking mutex");
        if (output) {
            flb_sds_destroy(output);
        }
        return NULL;
    }

    return output;
}

/**
 * Executes a control command against kusto's endpoint
 *
 * @param ctx       Plugin's context
 * @param csl       Kusto's control command
 * @return flb_sds_t      Returns the response or NULL on error.
 */
flb_sds_t execute_ingest_csl_command(struct flb_azure_kusto *ctx, const char *csl)
{
    flb_sds_t token;
    flb_sds_t body;
    size_t b_sent;
    int ret;
    struct flb_connection *u_conn;
    struct flb_http_client *c;
    flb_sds_t resp = NULL;

    flb_plg_debug(ctx->ins, "before getting upstream connection");

    flb_plg_debug(ctx->ins, "Logging attributes of flb_azure_kusto_resources:");
    flb_plg_debug(ctx->ins, "blob_ha: %p", ctx->resources->blob_ha);
    flb_plg_debug(ctx->ins, "queue_ha: %p", ctx->resources->queue_ha);
    flb_plg_debug(ctx->ins, "identity_token: %s", ctx->resources->identity_token);
    flb_plg_debug(ctx->ins, "load_time: %lu", ctx->resources->load_time);

    ctx->u->base.net.connect_timeout = ctx->ingestion_endpoint_connect_timeout ;

    /* Get upstream connection */
    u_conn = flb_upstream_conn_get(ctx->u);

    if (u_conn) {
        token = get_azure_kusto_token(ctx);
	    flb_plg_debug(ctx->ins, "after get azure kusto token");

        if (token) {
            /* Compose request body */
            body = flb_sds_create_size(sizeof(FLB_AZURE_KUSTO_MGMT_BODY_TEMPLATE) - 1 +
                                       strlen(csl));

            if (body) {
                flb_sds_snprintf(&body, flb_sds_alloc(body),
                                 FLB_AZURE_KUSTO_MGMT_BODY_TEMPLATE, csl);

                /* Compose HTTP Client request */
                c = flb_http_client(u_conn, FLB_HTTP_POST, FLB_AZURE_KUSTO_MGMT_URI_PATH,
                                    body, flb_sds_len(body), NULL, 0, NULL, 0);

                if (c) {
                    /* Add headers */
                    flb_http_add_header(c, "User-Agent", 10, "Fluent-Bit", 10);
                    flb_http_add_header(c, "Content-Type", 12, "application/json", 16);
                    flb_http_add_header(c, "Accept", 6, "application/json", 16);
                    flb_http_add_header(c, "Authorization", 13, token,
                                        flb_sds_len(token));
                    flb_http_add_header(c, "x-ms-client-version", 19, "Kusto.Fluent-Bit:1.0.0", 22);
                    flb_http_add_header(c, "x-ms-app", 8, "Kusto.Fluent-Bit", 16);
                    flb_http_add_header(c, "x-ms-user", 9, "Kusto.Fluent-Bit", 16);
                    flb_http_buffer_size(c, FLB_HTTP_DATA_SIZE_MAX * 10);

                    /* Send HTTP request */
                    ret = flb_http_do(c, &b_sent);
                    flb_plg_debug(
                        ctx->ins,
                        "Kusto ingestion command request http_do=%i, HTTP Status: %i",
                        ret, c->resp.status);

                    if (ret == 0) {
                        if (c->resp.status == 200) {
                            /* Copy payload response to the response param */
                            resp =
                                flb_sds_create_len(c->resp.payload, c->resp.payload_size);
                        }
                        else if (c->resp.payload_size > 0) {
                            flb_plg_debug(ctx->ins, "Request failed and returned: \n%s",
                                          c->resp.payload);
                        }
                        else {
                            flb_plg_debug(ctx->ins, "Request failed");
                        }
                    }
                    else {
                        flb_plg_error(ctx->ins, "cannot send HTTP request");
                    }

                    flb_http_client_destroy(c);
                }
                else {
                    flb_plg_error(ctx->ins, "cannot create HTTP client context");
                }

                flb_sds_destroy(body);
            }
            else {
                flb_plg_error(ctx->ins, "cannot construct request body");
            }

            flb_sds_destroy(token);
        }
        else {
            flb_plg_error(ctx->ins, "cannot retrieve oauth2 token");
        }

        flb_upstream_conn_release(u_conn);
    }
    else {
        flb_plg_error(ctx->ins, "cannot create upstream connection");
    }

    return resp;
}


static void add_comma_to_beginning(flb_sds_t *data) {
    size_t len = flb_sds_len(*data);

    // Resize the string to accommodate the comma
    *data = flb_sds_increase(*data, 1);

    // Shift the existing characters to the right by one position
    memmove(*data + 1, *data, len);

    // Add the comma at the beginning
    (*data)[0] = ',';

    // Update the string length
    flb_sds_len_set(*data, len + 1);
}

/*
 * Either new_data or chunk can be NULL, but not both
 */
static int construct_request_buffer(struct flb_azure_kusto *ctx, flb_sds_t new_data,
                                    struct azure_kusto_file *upload_file,
                                    char **out_buf, size_t *out_size)
{
    char *body;
    char *tmp;
    size_t body_size = 0;
    char *buffered_data = NULL;
    size_t buffer_size = 0;
    int ret;

    if (new_data == NULL && upload_file == NULL) {
        flb_plg_error(ctx->ins, "[construct_request_buffer] Something went wrong"
                                " both chunk and new_data are NULL");
        return -1;
    }

    if (upload_file) {
        ret = azure_kusto_store_file_upload_read(ctx, upload_file->fsf, &buffered_data, &buffer_size);
        if (ret < 0) {
            flb_plg_error(ctx->ins, "Could not read locally buffered data %s",
                          upload_file->fsf->name);
            return -1;
        }

        /*
         * lock the upload_file from buffer list
         */
        azure_kusto_store_file_lock(upload_file);
        body = buffered_data;
        body_size = buffer_size;
    }

    flb_plg_debug(ctx->ins, "[construct_request_buffer] size of buffer file read %zu", buffer_size);

    /*
     * If new data is arriving, increase the original 'buffered_data' size
     * to append the new one.
     */
    if (new_data) {
        body_size += flb_sds_len(new_data);
        flb_plg_debug(ctx->ins, "[construct_request_buffer] size of new_data %zu", body_size);

        tmp = flb_realloc(buffered_data, body_size + 1);
        if (!tmp) {
            flb_errno();
            flb_free(buffered_data);
            if (upload_file) {
                azure_kusto_store_file_unlock(upload_file);
            }
            return -1;
        }
        body = buffered_data = tmp;
        memcpy(body + buffer_size, new_data, flb_sds_len(new_data));
        if (ctx->compression_enabled == FLB_FALSE){
            body[body_size] = '\0';
        }
    }

    flb_plg_debug(ctx->ins, "[construct_request_buffer] final increased %zu", body_size);

    *out_buf = body;
    *out_size = body_size;

    return 0;
}


static int cb_azure_kusto_ingest(struct flb_config *config, void *data)
{
    struct flb_azure_kusto *ctx = data;
    struct azure_kusto_file *file = NULL;
    struct flb_fstore_file *fsf;
    char *buffer = NULL;
    size_t buffer_size = 0;
    struct mk_list *tmp;
    struct mk_list *head;
    int ret;
    time_t now;
    flb_sds_t payload;
    flb_sds_t tag_sds;

    flb_plg_debug(ctx->ins, "Running upload timer callback (cb_azure_kusto_ingest)..");

    now = time(NULL);

    /* Check all chunks and see if any have timed out */
    mk_list_foreach_safe(head, tmp, &ctx->stream_active->files) {
        fsf = mk_list_entry(head, struct flb_fstore_file, _head);
        file = fsf->data;
        flb_plg_debug(ctx->ins, "Iterating files inside upload timer callback (cb_azure_kusto_ingest).. %s", file->fsf->name);
        if (now < (file->create_time + ctx->upload_timeout + ctx->retry_time)) {
            continue; /* Only send chunks which have timed out */
        }

        flb_plg_debug(ctx->ins, "cb_azure_kusto_ingest :: Before file locked check %s", file->fsf->name);
        /* Locked chunks are being processed, skip */
        if (file->locked == FLB_TRUE) {
            continue;
        }

        flb_plg_debug(ctx->ins, "cb_azure_kusto_ingest :: Before construct_request_buffer %s", file->fsf->name);
        ret = construct_request_buffer(ctx, NULL, file, &buffer, &buffer_size);
        if (ret < 0) {
            flb_plg_error(ctx->ins, "Could not construct request buffer for %s",
                          file->fsf->name);
            continue;
        }

        payload = flb_sds_create(buffer);
        tag_sds = flb_sds_create(fsf->meta_buf);

        flb_plg_debug(ctx->ins, "cb_azure_kusto_ingest ::: tag of the file %s", tag_sds);

        ret = azure_kusto_load_ingestion_resources(ctx, config);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot load ingestion resources");
            return -1;
        }

        flb_plg_debug(ctx->ins, "cb_azure_kusto_ingest ::: before starting kusto queued ingestion %s", file->fsf->name);

        ret = azure_kusto_queued_ingestion(ctx, tag_sds, flb_sds_len(tag_sds), payload, flb_sds_len(payload));
        if (ret != 0) {
            flb_plg_error(ctx->ins, "Failed to ingest data to Azure Blob");
        }

        if (ret == 0) {
            flb_plg_debug(ctx->ins, "deleted successfully ingested file %s", fsf->name);
            flb_fstore_file_delete(ctx->fs, fsf);
        }

        flb_free(buffer);
        flb_sds_destroy(payload);
        flb_sds_destroy(tag_sds);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "Could not send chunk with tag %s",
                          (char *) fsf->meta_buf);
        }
    }
    flb_plg_debug(ctx->ins, "Exited upload timer callback (cb_azure_kusto_ingest)..");
    return ret;
}

void add_brackets_sds(flb_sds_t *data) {
    size_t len = flb_sds_len(*data);
    // Remove trailing comma if present
    if (len > 0 && (*data)[len - 1] == ',') {
        flb_sds_len_set(*data, len - 1);
    }
    flb_sds_t tmp = flb_sds_create("[");
    if (!tmp) {
        return; // Handle allocation failure
    }

    // Concatenate the existing data (now without the trailing comma)
    tmp = flb_sds_cat(tmp, *data, flb_sds_len(*data));
    if (!tmp) {
        return; // Handle possible reallocation failure
    }

    // Append the closing bracket
    tmp = flb_sds_cat(tmp, "]", 1);
    if (!tmp) {
        return; // Handle possible reallocation failure
    }

    // Destroy the old data and update the pointer
    flb_sds_destroy(*data);
    *data = tmp;
}

/* ingest data to Azure Kusto */
static int ingest_to_kusto_ext(void *out_context, flb_sds_t new_data,
                               struct azure_kusto_file *upload_file,
                               const char *tag, int tag_len)
{
    int ret;
    char *buffer;
    size_t buffer_size;
    struct flb_azure_kusto *ctx = out_context;
    flb_sds_t payload;
    flb_sds_t tag_sds = flb_sds_create_len(tag, tag_len);
    void *final_payload = NULL;
    size_t final_payload_size = 0;
    int is_compressed = FLB_FALSE;

    if (pthread_mutex_lock(&ctx->buffer_mutex)) {
        flb_plg_error(ctx->ins, "error unlocking mutex");
        return -1;
    }

    /* Create buffer */
    ret = construct_request_buffer(ctx, new_data, upload_file, &buffer, &buffer_size);
    if (ret < 0) {
        flb_plg_error(ctx->ins, "Could not construct request buffer for %s",
                      upload_file->fsf->name);
        return -1;
    }

    payload = flb_sds_create_len(buffer, buffer_size);
    flb_free(buffer);

    /* modify the payload to add brackets and remove trailing comma to make a json array ready for ingestion */
    add_brackets_sds(&payload);


    /* Compress the JSON payload */
    if (ctx->compression_enabled == FLB_TRUE) {
        ret = flb_gzip_compress((void *) payload, flb_sds_len(payload),
                                &final_payload, &final_payload_size);
        if (ret != 0) {
            flb_plg_error(ctx->ins,
                          "cannot gzip payload");
            flb_sds_destroy(payload);
            return -1;
        }
        else {
            is_compressed = FLB_TRUE;
            flb_plg_debug(ctx->ins, "enabled payload gzip compression");
            /* JSON buffer will be cleared at cleanup: */
        }
    } else {
        final_payload = payload;
        final_payload_size = flb_sds_len(payload);
    }

    if (pthread_mutex_unlock(&ctx->buffer_mutex)) {
        flb_plg_error(ctx->ins, "error unlocking mutex");
        return -1;
    }

    // Call azure_kusto_queued_ingestion to ingest the payload
    ret = azure_kusto_queued_ingestion(ctx, tag_sds, flb_sds_len(tag_sds), final_payload, final_payload_size);
    if (ret != 0) {
        flb_plg_error(ctx->ins, "Failed to ingest data to Azure Blob");
        flb_sds_destroy(tag_sds);
        flb_sds_destroy(payload);
        return -1;
    }

    flb_sds_destroy(tag_sds);
    flb_sds_destroy(payload);
    /* release compressed payload */
    if (is_compressed == FLB_TRUE) {
        flb_free(final_payload);
    }

    return 0;
}

/* Helper function to check if a JSON object is valid */
int is_valid_json(const char *json_str) {
    cJSON *json = cJSON_Parse(json_str);

    if (json == NULL) {
        return 0; // Invalid JSON
    }

    cJSON_Delete(json); // Clean up JSON object
    return 1; // Valid JSON
}


static int cb_azure_kusto_init(struct flb_output_instance *ins, struct flb_config *config,
                               void *data)
{
    int type;
    int io_flags = FLB_IO_TLS;
    struct flb_azure_kusto *ctx;
    struct flb_sched *sched;
    struct flb_fstore *fs;
    flb_sds_t tmp_sds;

    flb_plg_debug(ins, "inside azure kusto init");

    /* Create config context */
    ctx = flb_azure_kusto_conf_create(ins, config);
    if (!ctx) {
        flb_plg_error(ins, "configuration failed");
        return -1;
    }

    if (ctx->buffering_enabled == FLB_TRUE) {
        ctx->ins = ins;
        ctx->retry_time = 0;
        ctx->store_dir_limit_size = FLB_AZURE_KUSTO_BUFFER_DIR_MAX_SIZE;
        ctx->has_old_buffers = azure_kusto_store_has_data(ctx);

        /* Initialize local storage */
        int ret = azure_kusto_store_init(ctx);
        if (ret == -1) {
            flb_plg_error(ctx->ins, "Failed to initialize kusto storage: %s",
                          ctx->store_dir);
            return -1;
        }

        /* validate 'total_file_size' */
        if (ctx->file_size <= 0) {
            flb_plg_error(ctx->ins, "Failed to parse upload_file_size");
            return -1;
        }
        if (ctx->file_size < 1000000) {
            flb_plg_error(ctx->ins, "upload_file_size must be at least 1MB");
            return -1;
        }
        if (ctx->file_size > MAX_FILE_SIZE) {
            flb_plg_error(ctx->ins, "Max total_file_size must be lower than %ld bytes", MAX_FILE_SIZE);
            return -1;
        }
        flb_plg_info(ctx->ins, "Using upload size %lu bytes", ctx->file_size);
    }

    flb_output_set_context(ins, ctx);

    /* Network mode IPv6 */
    if (ins->host.ipv6 == FLB_TRUE) {
        io_flags |= FLB_IO_IPV6;
    }

    /* Create mutex for acquiring oauth tokens  and getting ingestion resources (they
     * are shared across flush coroutines)
     */
    pthread_mutex_init(&ctx->token_mutex, NULL);
    pthread_mutex_init(&ctx->resources_mutex, NULL);
    pthread_mutex_init(&ctx->blob_mutex, NULL);
    pthread_mutex_init(&ctx->buffer_mutex, NULL);

    /*
     * Create upstream context for Kusto Ingestion endpoint
     */
    ctx->u = flb_upstream_create_url(config, ctx->ingestion_endpoint, io_flags, ins->tls);
    if (!ctx->u) {
        flb_plg_error(ctx->ins, "upstream creation failed");
        return -1;
    }

    /* Create oauth2 context */
    ctx->o =
        flb_oauth2_create(ctx->config, ctx->oauth_url, FLB_AZURE_KUSTO_TOKEN_REFRESH);
    if (!ctx->o) {
        flb_plg_error(ctx->ins, "cannot create oauth2 context");
        return -1;
    }
    flb_output_upstream_set(ctx->u, ins);

    flb_plg_debug(ctx->ins, "azure kusto init completed");

    return 0;
}

static int azure_kusto_format(struct flb_azure_kusto *ctx, const char *tag, int tag_len,
                              const void *data, size_t bytes, void **out_data,
                              size_t *out_size)
{
    int records = 0;
    msgpack_sbuffer mp_sbuf;
    msgpack_packer mp_pck;
    /* for sub msgpack objs */
    int map_size;
    struct tm tms;
    char time_formatted[32];
    size_t s;
    int len;
    struct flb_log_event_decoder log_decoder;
    struct flb_log_event         log_event;
    int                          ret;
    /* output buffer */
    flb_sds_t out_buf;

    /* Create array for all records */
    records = flb_mp_count(data, bytes);
    if (records <= 0) {
        flb_plg_error(ctx->ins, "error counting msgpack entries");
        return -1;
    }

    ret = flb_log_event_decoder_init(&log_decoder, (char *) data, bytes);

    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ctx->ins,
                      "Log event decoder initialization error : %d", ret);

        return -1;
    }

    /* Create temporary msgpack buffer */
    msgpack_sbuffer_init(&mp_sbuf);
    msgpack_packer_init(&mp_pck, &mp_sbuf, msgpack_sbuffer_write);

    msgpack_pack_array(&mp_pck, records);

    while ((ret = flb_log_event_decoder_next(
                    &log_decoder,
                    &log_event)) == FLB_EVENT_DECODER_SUCCESS) {
        map_size = 1;
        if (ctx->include_time_key == FLB_TRUE) {
            map_size++;
        }

        if (ctx->include_tag_key == FLB_TRUE) {
            map_size++;
        }

        msgpack_pack_map(&mp_pck, map_size);

        /* include_time_key */
        if (ctx->include_time_key == FLB_TRUE) {
            msgpack_pack_str(&mp_pck, flb_sds_len(ctx->time_key));
            msgpack_pack_str_body(&mp_pck, ctx->time_key, flb_sds_len(ctx->time_key));

            /* Append the time value as ISO 8601 */
            gmtime_r(&log_event.timestamp.tm.tv_sec, &tms);
            s = strftime(time_formatted, sizeof(time_formatted) - 1,
                         FLB_PACK_JSON_DATE_ISO8601_FMT, &tms);

            len = snprintf(time_formatted + s, sizeof(time_formatted) - 1 - s,
                           ".%03" PRIu64 "Z",
                           (uint64_t)log_event.timestamp.tm.tv_nsec / 1000000);
            s += len;
            msgpack_pack_str(&mp_pck, s);
            msgpack_pack_str_body(&mp_pck, time_formatted, s);
        }

        /* include_tag_key */
        if (ctx->include_tag_key == FLB_TRUE) {
            msgpack_pack_str(&mp_pck, flb_sds_len(ctx->tag_key));
            msgpack_pack_str_body(&mp_pck, ctx->tag_key, flb_sds_len(ctx->tag_key));
            msgpack_pack_str(&mp_pck, tag_len);
            msgpack_pack_str_body(&mp_pck, tag, tag_len);
        }

        msgpack_pack_str(&mp_pck, flb_sds_len(ctx->log_key));
        msgpack_pack_str_body(&mp_pck, ctx->log_key, flb_sds_len(ctx->log_key));

        /* Check if log_event.body is available */
        if (log_event.body != NULL) {
            msgpack_pack_object(&mp_pck, *log_event.body);
        } else {
            /* Handle missing "log" attribute */
            /* Pack a default value */
            msgpack_pack_str(&mp_pck, 20);
            msgpack_pack_str_body(&mp_pck, "log_attribute_missing", 20);
        }
    }

    /* Convert from msgpack to JSON */
    out_buf = flb_msgpack_raw_to_json_sds(mp_sbuf.data, mp_sbuf.size);

    /* Cleanup */
    flb_log_event_decoder_destroy(&log_decoder);
    msgpack_sbuffer_destroy(&mp_sbuf);

    if (!out_buf) {
        flb_plg_error(ctx->ins, "error formatting JSON payload");
        return -1;
    }

    *out_data = out_buf;
    *out_size = flb_sds_len(out_buf);

    return 0;
}

static int buffer_chunk(void *out_context, struct azure_kusto_file *upload_file,
                        flb_sds_t chunk, int chunk_size,
                        flb_sds_t tag, size_t tag_len)
{
    int ret;
    struct flb_azure_kusto *ctx = out_context;

    flb_plg_trace(ctx->ins, "Buffering chunk %d", chunk_size);

    ret = azure_kusto_store_buffer_put(ctx, upload_file, tag,
                              tag_len, chunk, chunk_size);
    if (ret < 0) {
        flb_plg_error(ctx->ins, "Could not buffer chunk. ");
        return -1;
    }
    return 0;
}

static void flush_init(void *out_context, struct flb_config *config)
{
    int ret;
    struct flb_azure_kusto *ctx = out_context;

    /* clean up any old buffers found on startup */
    if (ctx->has_old_buffers == FLB_TRUE) {
        flb_plg_info(ctx->ins,
                     "Sending locally buffered data from previous "
                     "executions to kusto; buffer=%s",
                     ctx->fs->root_path);
        ctx->has_old_buffers = FLB_FALSE;
        ret = cb_azure_kusto_ingest(config, ctx);
        if (ret < 0) {
            ctx->has_old_buffers = FLB_TRUE;
            flb_plg_error(ctx->ins,
                          "Failed to send locally buffered data left over "
                          "from previous executions; will retry. Buffer=%s",
                          ctx->fs->root_path);
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }
    }
}

static void remove_brackets_sds(flb_sds_t *data) {
    size_t len = flb_sds_len(*data);

    if (len >= 2 && (*data)[0] == '[' && (*data)[len - 1] == ']') {
        // Shift the characters to the left by one position
        memmove(*data, *data + 1, len - 2);
        // Set the new length, removing the two bracket characters
        flb_sds_len_set(*data, len - 2);
        // Append a comma to the end of the modified string
        *data = flb_sds_cat(*data, ",", 1);
        if (*data == NULL) {
            // Handle possible reallocation failure
            return;
        }
    }
}

int lock_and_delete(const char *filePath) {
    // Open the file
    int fd = open(filePath, O_RDWR);
    if (fd == -1) {
        perror("Failed to open file");
        return -1;
    }

    // Acquire an exclusive lock on the file
    if (flock(fd, LOCK_EX) == -1) {
        perror("Failed to lock file");
        close(fd);
        return -1;
    }

    // Perform file deletion
    if (unlink(filePath) == -1) {
        perror("Failed to delete file");
        // Release the lock before returning
        flock(fd, LOCK_UN);
        close(fd);
        return -1;
    }

    // Release the lock
    if (flock(fd, LOCK_UN) == -1) {
        perror("Failed to unlock file");
        close(fd);
        return -1;
    }

    // Close the file descriptor
    close(fd);
    return 0;
}

static void cb_azure_kusto_flush(struct flb_event_chunk *event_chunk,
                                 struct flb_output_flush *out_flush,
                                 struct flb_input_instance *i_ins, void *out_context,
                                 struct flb_config *config)
{
    int ret;
    flb_sds_t json;
    size_t json_size;
    size_t tag_len;
    struct flb_azure_kusto *ctx = out_context;
    int is_compressed = FLB_FALSE;
    struct azure_kusto_file *upload_file = NULL;
    int upload_timeout_check = FLB_FALSE;
    int total_file_size_check = FLB_FALSE;

    (void)i_ins;
    (void)config;

    void *final_payload = NULL;
    size_t final_payload_size = 0;

    flb_plg_trace(ctx->ins, "flushing bytes %zu", event_chunk->size);

    tag_len = flb_sds_len(event_chunk->tag);

    if (ctx->buffering_enabled == FLB_TRUE) {

        flush_init(ctx,config);

        flb_plg_debug(ctx->ins,"event tag is  ::: %s", event_chunk->tag);

        if (pthread_mutex_lock(&ctx->buffer_mutex)) {
            flb_plg_error(ctx->ins, "error locking mutex");
            FLB_OUTPUT_RETURN(FLB_ERROR);
        }

        /* Reformat msgpack to JSON payload */
        ret = azure_kusto_format(ctx, event_chunk->tag, tag_len, event_chunk->data,
                                 event_chunk->size, (void **)&json, &json_size);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot reformat data into json");
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }

        /* Check if the JSON data is an array and valid json*/
        cJSON *root = cJSON_ParseWithLength((char*)json,json_size);
        if (root == NULL) {
            flb_plg_error(ctx->ins, "JSON parse error occurred for tag %s", event_chunk->tag);
            const char *error_ptr = cJSON_GetErrorPtr();
            if (error_ptr != NULL) {
                flb_plg_error(ctx->ins, "JSON parse error before: %s", error_ptr);
            }
            flb_sds_destroy(json);
            cJSON_Delete(root);
            FLB_OUTPUT_RETURN(FLB_ERROR);
        }

        cJSON_Delete(root);

        if (json_size >= 2 && json[0] == '[' && json[json_size - 1] == ']') {
            // Reduce 'bytes' by 1 to remove the ']' at the end
            json_size--;

            // Perform the shift to remove the '[' at the beginning
            memmove(json, json + 1, json_size - 2);
            json_size--;  // Adjust bytes to account for the removal of '['

            // Set the new length of the data string
            flb_sds_len_set(json, json_size);

            // Add a comma to the end
            json = flb_sds_cat(json, ",", 1);
            json_size = flb_sds_len(json);
        }else{
            flb_plg_warn(ctx->ins, "data from event chunk is not an json array or empty in json chunk %s", event_chunk->tag);
        }

        /* Get a file candidate matching the given 'tag' */
        upload_file = azure_kusto_store_file_get(ctx,
                                        event_chunk->tag,
                                                 tag_len);

        /* Discard upload_file if it has failed to upload MAX_UPLOAD_ERRORS times */
        if (upload_file != NULL && upload_file->failures >= MAX_UPLOAD_ERRORS) {
            flb_plg_warn(ctx->ins, "File with tag %s failed to send %d times, will not "
                                   "retry", event_chunk->tag, MAX_UPLOAD_ERRORS);
            azure_kusto_store_file_inactive(ctx, upload_file);
            upload_file = NULL;
        }

        /* If upload_timeout has elapsed, upload file */
        if (upload_file != NULL && time(NULL) >
                                   (upload_file->create_time + ctx->upload_timeout)) {
            upload_timeout_check = FLB_TRUE;
            flb_plg_trace(ctx->ins, "upload_timeout reached for %s",
                         event_chunk->tag);
        }

        /* If total_file_size has been reached, upload file */
        if (upload_file && upload_file->size + json_size > ctx->file_size) {
            flb_plg_trace(ctx->ins, "total_file_size exceeded %s",
                         event_chunk->tag);
            total_file_size_check = FLB_TRUE;
        }

        if (pthread_mutex_unlock(&ctx->buffer_mutex)) {
            flb_plg_error(ctx->ins, "error unlocking mutex");
            FLB_OUTPUT_RETURN(FLB_ERROR);
        }

        /* File is ready for upload, upload_file != NULL prevents from segfaulting. */
        if ((upload_file != NULL) && (upload_timeout_check == FLB_TRUE || total_file_size_check == FLB_TRUE)) {
            flb_plg_debug(ctx->ins, "uploading file %s with size %zu", upload_file->fsf->name, upload_file->size);
            //azure_kusto_store_file_lock(upload_file);
            /* Load or refresh ingestion resources */
            ret = azure_kusto_load_ingestion_resources(ctx, config);
            if (ret != 0) {
                flb_plg_error(ctx->ins, "cannot load ingestion resources");
                FLB_OUTPUT_RETURN(FLB_RETRY);
            }

            //upload_file = azure_kusto_store_file_get(ctx,
            //                                         event_chunk->tag,
            //                                         event_chunk->size);

            /* Send upload directly without upload queue */
            ret = ingest_to_kusto_ext(ctx, json, upload_file,
                                      event_chunk->tag,
                                      flb_sds_len(event_chunk->tag));
            if (ret == 0){
                if (pthread_mutex_lock(&ctx->buffer_mutex)) {
                    flb_plg_error(ctx->ins, "error locking mutex");
                    FLB_OUTPUT_RETURN(FLB_ERROR);
                }
                ret = azure_kusto_store_file_delete(ctx, upload_file);
                if (pthread_mutex_unlock(&ctx->buffer_mutex)) {
                    flb_plg_error(ctx->ins, "error unlocking mutex");
                    FLB_OUTPUT_RETURN(FLB_ERROR);
                }
                if (ret != 0){
                    flb_plg_error(ctx->ins, "file is ingested but unable to delete it %s with size %zu", upload_file->fsf->name, upload_file->size);
                    flb_sds_destroy(json);
                    FLB_OUTPUT_RETURN(FLB_ERROR);
                } else{
                    flb_plg_debug(ctx->ins, "successfully ingested & deleted file %s with size %zu", upload_file->fsf->name, upload_file->size);
                    flb_sds_destroy(json);
                    FLB_OUTPUT_RETURN(FLB_OK);
                }
            }else{
                flb_plg_error(ctx->ins, "unable to ingest file ");
                //azure_kusto_store_file_unlock(upload_file);
                flb_sds_destroy(json);
                // *** RELEASE THE LOCK HERE ***
                /*if (flock(upload_file->lock_fd, LOCK_UN) == -1) {
                    flb_plg_error(ctx->ins, "Failed to unlock file '%s': %s", upload_file->fsf->name, strerror(errno));
                }*/
                //close(upload_file->lock_fd);
                FLB_OUTPUT_RETURN(FLB_ERROR);
            }
        }

        //upload_file = azure_kusto_store_file_get_and_lock(ctx,
        //                                         event_chunk->tag,
        //                                         event_chunk->size);

        /* Buffer current chunk in filesystem and wait for next chunk from engine */
        ret = buffer_chunk(ctx, upload_file, json, json_size,
                           event_chunk->tag, flb_sds_len(event_chunk->tag));


        if (ret == 0) {
            flb_plg_debug(ctx->ins, "buffered chunk %s", event_chunk->tag);
            flb_sds_destroy(json);
            FLB_OUTPUT_RETURN(FLB_OK);
        } else {
            flb_plg_error(ctx->ins, "failed to buffer chunk %s", event_chunk->tag);
            flb_sds_destroy(json);
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }

    } else {
        /* Buffering mode is disabled, proceed with regular flush */

        /* Reformat msgpack to JSON payload */
        ret = azure_kusto_format(ctx, event_chunk->tag, tag_len, event_chunk->data,
                                 event_chunk->size, (void **)&json, &json_size);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot reformat data into json");
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }

        flb_plg_trace(ctx->ins, "payload size before compression %zu", json_size);
        /* Map buffer */
        final_payload = json;
        final_payload_size = json_size;
        if (ctx->compression_enabled == FLB_TRUE) {
            ret = flb_gzip_compress((void *) json, json_size,
                                    &final_payload, &final_payload_size);
            if (ret != 0) {
                flb_plg_error(ctx->ins,
                              "cannot gzip payload");
                flb_sds_destroy(json);
                FLB_OUTPUT_RETURN(FLB_RETRY);
            }
            else {
                is_compressed = FLB_TRUE;
                flb_plg_debug(ctx->ins, "enabled payload gzip compression");
                /* JSON buffer will be cleared at cleanup: */
            }
        }
        flb_plg_trace(ctx->ins, "payload size after compression %zu", final_payload_size);

        /* Load or refresh ingestion resources */
        ret = azure_kusto_load_ingestion_resources(ctx, config);
        flb_plg_trace(ctx->ins, "after flushing bytes xxxx  %d", ret);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot load ingestion resources");
            flb_sds_destroy(json);
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }

        ret = azure_kusto_queued_ingestion(ctx, event_chunk->tag, tag_len, final_payload, final_payload_size);
        flb_plg_trace(ctx->ins, "after kusto queued ingestion %d", ret);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot perform queued ingestion");
            flb_sds_destroy(json);
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }

        /* Cleanup */
        flb_sds_destroy(json);

        /* release compressed payload */
        if (is_compressed == FLB_TRUE) {
            flb_free(final_payload);
        }

        /* Done */
        FLB_OUTPUT_RETURN(FLB_OK);
    }
}


static int cb_azure_kusto_exit(void *data, struct flb_config *config)
{
    struct flb_azure_kusto *ctx = data;

    if (!ctx) {
        return -1;
    }

    if (ctx->u) {
        flb_upstream_destroy(ctx->u);
        ctx->u = NULL;
    }


    // Destroy the mutexes
    pthread_mutex_destroy(&ctx->resources_mutex);
    pthread_mutex_destroy(&ctx->token_mutex);
    pthread_mutex_destroy(&ctx->blob_mutex);
    pthread_mutex_destroy(&ctx->buffer_mutex);

    azure_kusto_store_exit(ctx);

    flb_azure_kusto_conf_destroy(ctx);

    return 0;
}

static struct flb_config_map config_map[] = {
    {FLB_CONFIG_MAP_STR, "tenant_id", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, tenant_id),
     "Set the tenant ID of the AAD application used for authentication"},
    {FLB_CONFIG_MAP_STR, "client_id", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, client_id),
     "Set the client ID (Application ID) of the AAD application used for authentication"},
    {FLB_CONFIG_MAP_STR, "client_secret", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, client_secret),
     "Set the client secret (Application Password) of the AAD application used for "
     "authentication"},
    {FLB_CONFIG_MAP_STR, "ingestion_endpoint", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, ingestion_endpoint),
     "Set the Kusto cluster's ingestion endpoint URL (e.g. "
     "https://ingest-mycluster.eastus.kusto.windows.net)"},
    {FLB_CONFIG_MAP_STR, "database_name", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, database_name), "Set the database name"},
    {FLB_CONFIG_MAP_STR, "table_name", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, table_name), "Set the table name"},
    {FLB_CONFIG_MAP_STR, "ingestion_mapping_reference", (char *)NULL, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, ingestion_mapping_reference),
     "Set the ingestion mapping reference"},
    {FLB_CONFIG_MAP_STR, "log_key", FLB_AZURE_KUSTO_DEFAULT_LOG_KEY, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, log_key), "The key name of event payload"},
    {FLB_CONFIG_MAP_BOOL, "include_tag_key", "true", 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, include_tag_key),
     "If enabled, tag is appended to output. "
     "The key name is used 'tag_key' property."},
    {FLB_CONFIG_MAP_STR, "tag_key", FLB_AZURE_KUSTO_DEFAULT_TAG_KEY, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, tag_key),
     "The key name of tag. If 'include_tag_key' is false, "
     "This property is ignored"},
    {FLB_CONFIG_MAP_BOOL, "include_time_key", "true", 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, include_time_key),
     "If enabled, time is appended to output. "
     "The key name is used 'time_key' property."},
    {FLB_CONFIG_MAP_STR, "time_key", FLB_AZURE_KUSTO_DEFAULT_TIME_KEY, 0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, time_key),
     "The key name of the time. If 'include_time_key' is false, "
     "This property is ignored"},
    {FLB_CONFIG_MAP_TIME, "ingestion_endpoint_connect_timeout", FLB_AZURE_KUSTO_INGEST_ENDPOINT_CONNECTION_TIMEOUT, 0, FLB_TRUE,
	         offsetof(struct flb_azure_kusto, ingestion_endpoint_connect_timeout),
		              "Set the ingestion endpoint connection timeout in seconds"},
    {FLB_CONFIG_MAP_BOOL, "compression_enabled", "true", 0, FLB_TRUE,
            offsetof(struct flb_azure_kusto, compression_enabled), "Enable HTTP payload compression (gzip)."
    },
    {FLB_CONFIG_MAP_BOOL, "buffering_enabled", "false", 0, FLB_TRUE,
            offsetof(struct flb_azure_kusto, buffering_enabled), "Enable buffering into disk before ingesting into Azure Kusto."
    },
    {FLB_CONFIG_MAP_STR, "buffer_dir", "/tmp/fluent-bit/azure-kusto/", 0, FLB_TRUE,
            offsetof(struct flb_azure_kusto, buffer_dir), "Specifies the location of directory where the buffered data will be stored."
    },
    {FLB_CONFIG_MAP_TIME, "upload_timeout", "30m",
            0, FLB_TRUE, offsetof(struct flb_azure_kusto, upload_timeout),
    "Optionally specify a timeout for uploads. "
    "Fluent Bit will start ingesting buffer files which have been created more than x minutes and haven't reached upload_file_size limit yet.  "
    " Default is 30m."
    },
    {FLB_CONFIG_MAP_SIZE, "upload_file_size", "200M",
            0, FLB_TRUE, offsetof(struct flb_azure_kusto, file_size),
    "Specifies the size of files to be uploaded in MBs. Default is 200MB"
    },
    {FLB_CONFIG_MAP_TIME, "ingestion_resources_refresh_interval", FLB_AZURE_KUSTO_RESOURCES_LOAD_INTERVAL_SEC,0, FLB_TRUE,
          offsetof(struct flb_azure_kusto, ingestion_resources_refresh_interval),
          "Set the azure kusto ingestion resources refresh interval"
    },
    {FLB_CONFIG_MAP_STR, "azure_kusto_buffer_key", "key",0, FLB_TRUE,
     offsetof(struct flb_azure_kusto, azure_kusto_buffer_key),
    "Set the azure kusto buffer key which needs to be specified when using multiple instances of azure kusto output plugin and buffering is enabled"
    },
    /* EOF */
    {0}};

struct flb_output_plugin out_azure_kusto_plugin = {
    .name = "azure_kusto",
    .description = "Send events to Kusto (Azure Data Explorer)",
    .cb_init = cb_azure_kusto_init,
    .cb_flush = cb_azure_kusto_flush,
    .cb_exit = cb_azure_kusto_exit,
    .config_map = config_map,
    /* Plugin flags */
    .flags = FLB_OUTPUT_NET | FLB_IO_TLS,
};
