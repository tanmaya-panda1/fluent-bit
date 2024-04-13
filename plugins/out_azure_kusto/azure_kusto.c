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
#include <fluent-bit/flb_kv.h>
#include <fluent-bit/flb_oauth2.h>
#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_signv4.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <fluent-bit/flb_scheduler.h>
#include <fluent-bit/flb_gzip.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <fluent-bit/flb_sds.h>
#include <fluent-bit/flb_fstore.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <stdlib.h>
#include <msgpack.h>

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

static flb_sds_t azure_kusto_format_file_name(struct flb_azure_kusto *ctx,
                                              const char *tag, int tag_len)
{
    struct flb_time tm;
    char time_str[64];
    int len;
    flb_sds_t file_name;

    flb_time_get(&tm);
    len = snprintf(time_str, sizeof(time_str) - 1,
                   "%04d%02d%02d%02d%02d%02d",
                   tm.tm.tv_sec / 86400, tm.tm.tv_sec / 3600 % 24,
                   tm.tm.tv_sec / 60 % 60, tm.tm.tv_sec % 60,
                   tm.tm.tv_nsec / 1000000, tm.tm.tv_nsec % 1000000);

    file_name = flb_sds_create_size(tag_len + len + 5);
    if (!file_name) {
        flb_errno();
        return NULL;
    }

    flb_sds_cat_safe(&file_name, tag, tag_len);
    flb_sds_cat_safe(&file_name, "_", 1);
    flb_sds_cat_safe(&file_name, time_str, len);
    flb_sds_cat_safe(&file_name, ".log", 4);

    return file_name;
}

static int azure_kusto_buffer_data(struct flb_azure_kusto *ctx,
                                   const char *tag, int tag_len,
                                   char *data, size_t bytes)
{
    int ret;
    flb_sds_t file_name;
    flb_sds_t file_path;
    FILE *fp;

    file_name = azure_kusto_format_file_name(ctx, tag, tag_len);
    if (!file_name) {
        flb_plg_error(ctx->ins, "failed to format file name");
        return -1;
    }

    file_path = flb_sds_create_size(flb_sds_len(ctx->buffer_dir) + flb_sds_len(file_name) + 2);
    if (!file_path) {
        flb_errno();
        flb_sds_destroy(file_name);
        return -1;
    }

    flb_sds_cat_safe(&file_path, ctx->buffer_dir, flb_sds_len(ctx->buffer_dir));
    flb_sds_cat_safe(&file_path, "/", 1);
    flb_sds_cat_safe(&file_path, file_name, flb_sds_len(file_name));
    flb_sds_destroy(file_name);

    fp = fopen(file_path, "ab");
    if (!fp) {
        flb_errno();
        flb_sds_destroy(file_path);
        return -1;
    }

    ret = fwrite(data, 1, bytes, fp);
    if (ret != bytes) {
        flb_errno();
        fclose(fp);
        flb_sds_destroy(file_path);
        return -1;
    }

    fclose(fp);
    flb_sds_destroy(file_path);

    return 0;
}

/*
 * return value is one of FLB_OK, FLB_RETRY, FLB_ERROR
 *
 * Chunk is allowed to be NULL
 *//*
static int upload_data(struct flb_azure_kusto *ctx, struct azure_kusto_file *chunk,
                       char *body, size_t body_size,
                       const char *tag, int tag_len)
{
    int init_upload = FLB_FALSE;
    int complete_upload = FLB_FALSE;
    int size_check = FLB_FALSE;
    int part_num_check = FLB_FALSE;
    int timeout_check = FLB_FALSE;
    int ret;
    void *payload_buf = NULL;
    size_t payload_size = 0;
    size_t preCompress_size = 0;
    time_t file_first_log_time = time(NULL);

    *//*
     * When chunk does not exist, file_first_log_time will be the current time.
     * This is only for unit tests and prevents unit tests from segfaulting when chunk is
     * NULL because if so chunk->first_log_time will be NULl either and will cause
     * segfault during the process of put_object upload or mutipart upload.
     *//*
    if (chunk != NULL) {
        file_first_log_time = chunk->first_log_time;
    }

    if (ctx->compression_enabled == FLB_TRUE) {
        *//* Map payload *//*
        ret = flb_aws_compression_compress(ctx->compression, body, body_size, &payload_buf, &payload_size);
        if (ret == -1) {
            flb_plg_error(ctx->ins, "Failed to compress data");
            return FLB_RETRY;
        } else {
            preCompress_size = body_size;
            body = (void *) payload_buf;
            body_size = payload_size;
        }
    }

    if (ctx->use_put_object == FLB_TRUE) {
        goto put_object;
    }


        if (chunk != NULL && time(NULL) >
                             (chunk->create_time + ctx->upload_timeout + ctx->retry_time)) {
            *//* timeout already reached, just PutObject *//*
            goto put_object;
        }
        else if (body_size >= ctx->file_size) {
            *//* already big enough, just use PutObject API *//*
            goto put_object;
        }
        else {
            if (ctx->use_put_object == FLB_FALSE && ctx->compression == FLB_AWS_COMPRESS_GZIP) {
                flb_plg_info(ctx->ins, "Pre-compression upload_chunk_size= %zu, After compression, chunk is only %zu bytes, "
                                       "the chunk was too small, using PutObject to upload", preCompress_size, body_size);
            }
            goto put_object;
        }

    put_object:

    *//*
     * remove chunk from buffer list
     *//*
    ret = s3_put_object(ctx, tag, file_first_log_time, body, body_size);
    if (ctx->compression == FLB_AWS_COMPRESS_GZIP) {
        flb_free(payload_buf);
    }
    if (ret < 0) {
        *//* re-add chunk to list *//*
        if (chunk) {
            s3_store_file_unlock(chunk);
            chunk->failures += 1;
        }
        return FLB_RETRY;
    }

    *//* data was sent successfully- delete the local buffer *//*
    if (chunk) {
        azure_kusto_store_file_delete(ctx, chunk);
    }

    return FLB_OK;
}*/

/*
 * Either new_data or chunk can be NULL, but not both
 */
static int construct_request_buffer(struct flb_azure_kusto *ctx, flb_sds_t new_data,
                                    struct azure_kusto_file *chunk,
                                    char **out_buf, size_t *out_size)
{
    char *body;
    char *tmp;
    size_t body_size = 0;
    char *buffered_data = NULL;
    size_t buffer_size = 0;
    int ret;

    if (new_data == NULL && chunk == NULL) {
        flb_plg_error(ctx->ins, "[construct_request_buffer] Something went wrong"
                                " both chunk and new_data are NULL");
        return -1;
    }

    if (chunk) {
        ret = azure_kusto_store_file_read(ctx, chunk, &buffered_data, &buffer_size);
        if (ret < 0) {
            flb_plg_error(ctx->ins, "Could not read locally buffered data %s",
                          chunk->file_path);
            return -1;
        }

        /*
         * lock the chunk from buffer list
         */
        azure_kusto_store_file_lock(chunk);
        body = buffered_data;
        body_size = buffer_size;
    }

    /*
     * If new data is arriving, increase the original 'buffered_data' size
     * to append the new one.
     */
    if (new_data) {
        body_size += flb_sds_len(new_data);

        tmp = flb_realloc(buffered_data, body_size + 1);
        if (!tmp) {
            flb_errno();
            flb_free(buffered_data);
            if (chunk) {
                azure_kusto_store_file_unlock(chunk);
            }
            return -1;
        }
        body = buffered_data = tmp;
        memcpy(body + buffer_size, new_data, flb_sds_len(new_data));
        body[body_size] = '\0';
    }

    *out_buf = body;
    *out_size = body_size;

    return 0;
}


static void cb_azure_kusto_ingest(struct flb_config *config, void *data)
{
    struct flb_azure_kusto *ctx = data;
    struct azure_kusto_file *chunk = NULL;
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
        chunk = fsf->data;

        if (now < (chunk->create_time + ctx->upload_timeout + ctx->retry_time)) {
            continue; /* Only send chunks which have timed out */
        }

        /* Locked chunks are being processed, skip */
        if (chunk->locked == FLB_TRUE) {
            continue;
        }

        ret = construct_request_buffer(ctx, NULL, chunk, &buffer, &buffer_size);
        if (ret < 0) {
            flb_plg_error(ctx->ins, "Could not construct request buffer for %s",
                          chunk->file_path);
            continue;
        }

        payload = flb_sds_create(buffer);
        tag_sds = flb_sds_create(fsf->meta_buf);

        ret = azure_kusto_load_ingestion_resources(ctx, config);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot load ingestion resources");
            FLB_OUTPUT_RETURN(FLB_ERROR);
        }

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
        if (ret != FLB_OK) {
            flb_plg_error(ctx->ins, "Could not send chunk with tag %s",
                          (char *) fsf->meta_buf);
        }
    }

}

// Timer callback to ingest data to Azure Kusto
static int ingest_to_kusto_ext(void *out_context, flb_sds_t chunk,
                               struct azure_kusto_file *upload_file,
                               const char *tag, int tag_len)
{
    int ret;
    char *buffer;
    size_t buffer_size;
    struct flb_azure_kusto *ctx = out_context;
    flb_sds_t payload;
    flb_sds_t tag_sds = flb_sds_create(tag);

    flb_plg_trace(ctx->ins, "inside ingest_to_kusto_ext ");

    /* Create buffer to upload to S3 */
    ret = construct_request_buffer(ctx, chunk, upload_file, &buffer, &buffer_size);
    flb_sds_destroy(chunk);
    if (ret < 0) {
        flb_plg_error(ctx->ins, "Could not construct request buffer for %s",
                      upload_file->file_path);
        return -1;
    }

    payload = flb_sds_create(buffer);

    //ret = azure_kusto_load_ingestion_resources(ctx, config);
    //if (ret != 0) {
    //    flb_plg_error(ctx->ins, "cannot load ingestion resources");
    //    return -1;
    //}

    // Call azure_kusto_queued_ingestion to ingest the payload
    ret = azure_kusto_queued_ingestion(ctx, tag_sds, flb_sds_len(tag_sds), payload, flb_sds_len(payload));
    if (ret != 0) {
        flb_plg_error(ctx->ins, "Failed to ingest data to Azure Blob");
        return -1;
    }

    return 0;
}


// Timer callback to ingest data to Azure Kusto
static int ingest_to_kusto(struct flb_config *config, struct flb_sched_timer *timer, void *data)
{
    struct flb_azure_kusto *ctx = data;
    struct mk_list *head;
    struct mk_list *tmp;
    struct flb_azure_buffer_file_metadata *metadata;
    time_t current_time = time(NULL);

    mk_list_foreach_safe(head, tmp, &ctx->buffer_files) {
        metadata = mk_list_entry(head, struct flb_azure_buffer_file_metadata, _head);

        // Check if file is not locked and exceeds the maximum file size or wait time
        if (!metadata->locked && (metadata->file_size > FLB_AZURE_KUSTO_BUFFER_MAX_FILE_SIZE ||
                                  current_time - metadata->last_modified_time > FLB_AZURE_KUSTO_BUFFER_MAX_FILE_WAIT_TIME)) {
            // Read the contents of the buffer file
            flb_sds_t payload = flb_sds_create_len(NULL, metadata->file_size);
            FILE *file = fopen(metadata->file_path, "rb");
            if (file) {
                size_t bytes_read = fread(payload, 1, metadata->file_size, file);
                if (bytes_read != metadata->file_size) {
                    flb_plg_error(ctx->ins, "Failed to read buffer file: %s", metadata->file_path);
                }
                fclose(file);
            } else {
                flb_plg_error(ctx->ins, "Failed to open buffer file: %s", metadata->file_path);
            }

            int ret = azure_kusto_load_ingestion_resources(ctx, config);
            if (ret != 0) {
                flb_plg_error(ctx->ins, "cannot load ingestion resources");
            }

            // Call azure_kusto_queued_ingestion to ingest the payload
            ret = azure_kusto_queued_ingestion(ctx, metadata->event_tag, metadata->tag_len, payload, metadata->file_size);
            if (ret != 0) {
                flb_plg_error(ctx->ins, "Failed to ingest data to Azure Blob");
            }

            // Remove the file from the buffer directory
            if (remove(metadata->file_path) == -1) {
                flb_plg_error(ctx->ins, "Failed to remove buffer file: %s", metadata->file_path);
            }

            // Remove the metadata from the list and free memory
            mk_list_del(&metadata->_head);
            flb_sds_destroy(metadata->file_path);
            flb_sds_destroy(metadata->event_tag);
            flb_free(metadata);

            // Free the payload
            flb_sds_destroy(payload);
        }
    }

    return 0;
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

        /* Initialize local storage */
        int ret = azure_kusto_store_init(ctx);
        if (ret == -1) {
            flb_plg_error(ctx->ins, "Failed to initialize kusto storage: %s",
                          ctx->store_dir);
            return -1;
        }

        ctx->timer_created = FLB_FALSE;
        /*ctx->timer_ms = (int) (ctx->upload_timeout / 6) * 1000;
        if (ctx->timer_ms > UPLOAD_TIMER_MAX_WAIT) {
            ctx->timer_ms = UPLOAD_TIMER_MAX_WAIT;
        }
        else if (ctx->timer_ms < UPLOAD_TIMER_MIN_WAIT) {
            ctx->timer_ms = UPLOAD_TIMER_MIN_WAIT;
        }*/

        ctx->timer_ms = 1800000;
        flb_plg_debug(ins, "final timer_ms is  %d", ctx->timer_ms);

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
            flb_plg_error(ctx->ins, "Max total_file_size is %ld bytes", MAX_FILE_SIZE);
            return -1;
        }
        flb_plg_info(ctx->ins, "Using upload size %lu bytes", ctx->file_size);


        /*mk_list_init(&ctx->buffer_files);
        ctx->current_file_path = NULL;
        ctx->current_file = NULL;
        ctx->current_file_size = 0;
        ctx->ins = ins;

        ctx->retry_time = 0;

        type = FLB_FSTORE_FS;

        fs = flb_fstore_create(ctx->buffer_dir, type);
        if (!fs) {
            return -1;
        }

        sched = flb_sched_ctx_get();

        // Start the timer for ingestion to Azure Kusto
        int ret = flb_sched_timer_cb_create(sched, FLB_SCHED_TIMER_CB_PERM, 6000, ingest_to_kusto, ctx, NULL);
        if (ret == -1) {
            flb_plg_error(ins, "Failed to start ingestion timer");
            flb_free(ctx);
            return -1;
        }*/
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
        msgpack_pack_object(&mp_pck, *log_event.body);
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

static FILE *flb_sds_create_file(flb_sds_t file_path, const char *mode, int bufsize)
{
    FILE *fp;

    fp = fopen(file_path, mode);
    if (!fp) {
        flb_errno();
        return NULL;
    }

    if (bufsize > 0) {
        setvbuf(fp, NULL, _IOFBF, bufsize);
    }

    return fp;
}

static int64_t get_current_time_milliseconds() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/*
 * Flushes the JSON data to a buffer file on disk
 */
static void azure_kusto_flush_to_buffer(const void *data, size_t bytes, const char *tag,
                                        int tag_len, struct flb_input_instance *i_ins,
                                        struct flb_azure_kusto *ctx, struct flb_config *config)
{
    flb_sds_t json_data = (flb_sds_t)data;
    int ret;

    // Check if the buffer directory size exceeds the limit
    struct stat st;
    if (stat(ctx->buffer_dir, &st) == 0) {
        if (st.st_size > FLB_AZURE_KUSTO_BUFFER_DIR_MAX_SIZE) {
            flb_plg_warn(ctx->ins, "Buffer directory size exceeds the limit");
        }
    }

    // Check if a buffer file exists and its size is within the limit
    if (ctx->current_file && ctx->current_file_size < FLB_AZURE_KUSTO_BUFFER_MAX_FILE_SIZE) {
        // Append JSON data to the current buffer file
        ret = flb_sds_cat(ctx->current_file_path, json_data, flb_sds_len(json_data));
        if (ret < 0) {
            flb_plg_error(ctx->ins, "Failed to write JSON data to buffer file: %s", ctx->current_file_path);
            return;
        }
        ctx->current_file_size += flb_sds_len(json_data);
        fflush(ctx->current_file);

        // Update the buffer file metadata
        struct flb_azure_buffer_file_metadata *metadata = flb_calloc(1, sizeof(struct flb_azure_buffer_file_metadata));
        if (metadata) {
            metadata->file_path = flb_sds_create(ctx->current_file_path);
            metadata->last_modified_time = time(NULL);
            metadata->file_size = ctx->current_file_size;
            metadata->event_tag = flb_sds_create_len(tag, tag_len);
            metadata->tag_len = tag_len;

            // Check if the current file size exceeds the maximum file size or if the last modified time exceeds the maximum file wait time
            if (ctx->current_file_size >= FLB_AZURE_KUSTO_BUFFER_MAX_FILE_SIZE || (time(NULL) - metadata->last_modified_time) >= FLB_AZURE_KUSTO_BUFFER_MAX_FILE_WAIT_TIME) {
                metadata->locked = 0;
            } else {
                metadata->locked = 1;
            }

            mk_list_add(&metadata->_head, &ctx->buffer_files);
        }
    } else {
        // Close the current buffer file if it exists
        if (ctx->current_file) {
            fclose(ctx->current_file);
            ctx->current_file = NULL;
        }

        // Create a new buffer file
        ctx->current_file_path = flb_sds_create_size(256);
        if (!ctx->current_file_path) {
            flb_plg_error(ctx->ins, "Failed to allocate buffer file path");
            return;
        }
        flb_sds_printf(&ctx->current_file_path, "%s/%s_%lld.buf", ctx->buffer_dir,
                       "fluent-bit-kusto", get_current_time_milliseconds());

        ctx->current_file = flb_sds_create_file(ctx->current_file_path, "wb", 0);
        if (!ctx->current_file) {
            flb_plg_error(ctx->ins, "Failed to open buffer file: %s", ctx->current_file_path);
            flb_sds_destroy(ctx->current_file_path);
            ctx->current_file_path = NULL;
            return;
        }

        // Write JSON data to the new buffer file
        ret = flb_sds_cat(ctx->current_file_path, json_data, flb_sds_len(json_data));
        if (ret < 0) {
            flb_plg_error(ctx->ins, "Failed to write JSON data to buffer file: %s", ctx->current_file_path);
            flb_sds_destroy(ctx->current_file);
            ctx->current_file = NULL;
            flb_sds_destroy(ctx->current_file_path);
            ctx->current_file_path = NULL;
            return;
        }
        ctx->current_file_size = flb_sds_len(json_data);

        // Create buffer file metadata
        struct flb_azure_buffer_file_metadata *metadata = flb_calloc(1, sizeof(struct flb_azure_buffer_file_metadata));
        if (metadata) {
            metadata->file_path = flb_sds_create(ctx->current_file_path);
            metadata->created_time = time(NULL);
            metadata->last_modified_time = metadata->created_time;
            metadata->file_size = ctx->current_file_size;
            metadata->event_tag = flb_sds_create_len(tag, tag_len);
            metadata->tag_len = tag_len;
            // Check if the current file size exceeds the maximum file size or if the last modified time exceeds the maximum file wait time
            if (ctx->current_file_size >= FLB_AZURE_KUSTO_BUFFER_MAX_FILE_SIZE || (time(NULL) - metadata->last_modified_time) >= FLB_AZURE_KUSTO_BUFFER_MAX_FILE_WAIT_TIME) {
                metadata->locked = 0;
            } else {
                metadata->locked = 1;
            }
            mk_list_add(&metadata->_head, &ctx->buffer_files);
        }
    }
}

static int buffer_chunk(void *out_context, struct azure_kusto_file *upload_file,
                        flb_sds_t chunk, int chunk_size,
                        const char *tag, int tag_len,
                        time_t file_first_log_time)
{
    int ret;
    struct flb_azure_kusto *ctx = out_context;

    flb_plg_trace(ctx->ins, "Buffering chunk %d", chunk_size);

    ret = azure_kusto_store_buffer_put(ctx, upload_file, tag,
                              tag_len, chunk, (size_t) chunk_size, file_first_log_time);
    flb_sds_destroy(chunk);
    if (ret < 0) {
        flb_plg_warn(ctx->ins, "Could not buffer chunk. ");
        return -1;
    }
    return 0;
}

static void flush_init(void *out_context)
{
    int ret;
    struct flb_azure_kusto *ctx = out_context;
    struct flb_sched *sched;

    /* clean up any old buffers found on startup */
    if (ctx->has_old_buffers == FLB_TRUE) {
        flb_plg_info(ctx->ins,
                     "Sending locally buffered data from previous "
                     "executions to kusto; buffer=%s",
                     ctx->fs->root_path);
        ctx->has_old_buffers = FLB_FALSE;
        /*ret = put_all_chunks(ctx);
        if (ret < 0) {
            ctx->has_old_buffers = FLB_TRUE;
            flb_plg_error(ctx->ins,
                          "Failed to send locally buffered data left over "
                          "from previous executions; will retry. Buffer=%s",
                          ctx->fs->root_path);
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }*/
    }

    /*
     * create a timer that will run periodically and check if uploads
     * are ready for completion
     * this is created once on the first flush
     */
    if (ctx->timer_created == FLB_FALSE) {
        flb_plg_debug(ctx->ins,
                      "Creating upload timer with frequency %ds",
                      ctx->timer_ms / 1000);

        sched = flb_sched_ctx_get();
        ret = flb_sched_timer_cb_create(sched, FLB_SCHED_TIMER_CB_PERM,
                                            ctx->timer_ms, cb_azure_kusto_ingest, ctx, NULL);
        if (ret == -1) {
            flb_plg_error(ctx->ins, "Failed to create upload timer");
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }
        ctx->timer_created = FLB_TRUE;
    }
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
    struct flb_log_event_decoder log_decoder;
    struct flb_log_event log_event;
    time_t file_first_log_time = 0;
    int upload_timeout_check = FLB_FALSE;
    int total_file_size_check = FLB_FALSE;

    (void)i_ins;
    (void)config;

    void *final_payload = NULL;
    size_t final_payload_size = 0;

    flb_plg_trace(ctx->ins, "flushing bytes %zu", event_chunk->size);

    tag_len = flb_sds_len(event_chunk->tag);

    /* Reformat msgpack to JSON payload */
    ret = azure_kusto_format(ctx, event_chunk->tag, tag_len, event_chunk->data,
                             event_chunk->size, (void **)&json, &json_size);
    flb_plg_trace(ctx->ins, "after kusto format xxxx %d", ret);
    if (ret != 0) {
        flb_plg_error(ctx->ins, "cannot reformat data into json");
        FLB_OUTPUT_RETURN(FLB_RETRY);
    }

    if (ctx->buffering_enabled == FLB_TRUE) {

        ret = azure_kusto_load_ingestion_resources(ctx, config);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "cannot load ingestion resources");
            FLB_OUTPUT_RETURN(FLB_ERROR);
        }

       // flush_init(ctx);

        flb_plg_debug(ctx->ins,"event tag is  ::: %s", event_chunk->tag);

        /* Get a file candidate matching the given 'tag' */
        upload_file = azure_kusto_store_file_get(ctx,
                                        event_chunk->tag,
                                        event_chunk->size);
        //flb_plg_debug(ctx->ins,"upload_file retrieved is  ::: %s", upload_file->);

        if (upload_file == NULL) {
            flb_plg_debug(ctx->ins, "upload_file is NULL or size exceeded, creating new file");
            ret = flb_log_event_decoder_init(&log_decoder,
                                             (char *) event_chunk->data,
                                             event_chunk->size);

            if (ret != FLB_EVENT_DECODER_SUCCESS) {
                flb_plg_error(ctx->ins,
                              "Log event decoder initialization error : %d", ret);

                flb_sds_destroy(json);

                FLB_OUTPUT_RETURN(FLB_ERROR);
            }

            while ((ret = flb_log_event_decoder_next(
                    &log_decoder,
                    &log_event)) == FLB_EVENT_DECODER_SUCCESS) {
                if (log_event.timestamp.tm.tv_sec != 0) {
                    file_first_log_time = log_event.timestamp.tm.tv_sec;
                    break;
                }
            }

            flb_log_event_decoder_destroy(&log_decoder);
        }
        else {
            /* Get file_first_log_time from upload_file */
            file_first_log_time = upload_file->first_log_time;
        }

        if (file_first_log_time == 0) {
            file_first_log_time = time(NULL);
        }

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
            flb_plg_info(ctx->ins, "upload_timeout reached for %s",
                         event_chunk->tag);
        }

        /* If total_file_size has been reached, upload file */
        if (upload_file && upload_file->size + json_size > ctx->file_size) {
            flb_plg_info(ctx->ins, "total_file_size exceeded %s",
                         event_chunk->tag);
            total_file_size_check = FLB_TRUE;
        }

        /* File is ready for upload, upload_file != NULL prevents from segfaulting. */
        if ((upload_file != NULL) && (upload_timeout_check == FLB_TRUE || total_file_size_check == FLB_TRUE)) {
            flb_plg_trace(ctx->ins, "before loading ingestion resources xxxx ");
            /* Load or refresh ingestion resources */
            ret = azure_kusto_load_ingestion_resources(ctx, config);
            if (ret != 0) {
                flb_plg_error(ctx->ins, "cannot load ingestion resources");
                FLB_OUTPUT_RETURN(FLB_ERROR);
            }

            /* Send upload directly without upload queue */
            ret = ingest_to_kusto_ext(ctx, json, upload_file,
                                      event_chunk->tag,
                                      flb_sds_len(event_chunk->tag));
            if (ret == 0){
                flb_plg_debug(ctx->ins, "successfully ingested and deleted file %s ", upload_file->file_path);
                azure_kusto_store_file_delete(ctx, upload_file);
            }
            if (ret < 0) {
                FLB_OUTPUT_RETURN(FLB_ERROR);
            }
            FLB_OUTPUT_RETURN(ret);
        }

        /* Buffer current chunk in filesystem and wait for next chunk from engine */
        ret = buffer_chunk(ctx, upload_file, json, json_size,
                           event_chunk->tag, flb_sds_len(event_chunk->tag),
                           file_first_log_time);

        if (ret < 0) {
            FLB_OUTPUT_RETURN(FLB_RETRY);
        }


        /* Buffering mode is enabled, call azure_kusto_flush_to_buffer */
        //azure_kusto_flush_to_buffer(json, json_size, event_chunk->tag, tag_len, i_ins, ctx, config);
        //flb_sds_destroy(json);
        FLB_OUTPUT_RETURN(FLB_OK);
    } else {
        /* Buffering mode is disabled, proceed with regular flush */
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
    }

    /* Done */
    FLB_OUTPUT_RETURN(FLB_OK);
}


static int cb_azure_kusto_exit(void *data, struct flb_config *config)
{
    struct flb_azure_kusto *ctx = data;

    struct mk_list *head;
    struct mk_list *tmp;
    struct flb_azure_buffer_file_metadata *metadata;

    if (!ctx) {
        return -1;
    }

    if (ctx->u) {
        flb_upstream_destroy(ctx->u);
        ctx->u = NULL;
    }

    // Close the current buffer file if it exists
    if (ctx->current_file) {
        fclose(ctx->current_file);
        ctx->current_file = NULL;
    }

    // Free the current file path
    if (ctx->current_file_path) {
        flb_sds_destroy(ctx->current_file_path);
        ctx->current_file_path = NULL;
    }
    
    // Free the buffer files
    mk_list_foreach_safe(head, tmp, &ctx->buffer_files) {
        metadata = mk_list_entry(head, struct flb_azure_buffer_file_metadata, _head);
        mk_list_del(&metadata->_head);
        flb_sds_destroy(metadata->file_path);
        flb_sds_destroy(metadata->event_tag);
        flb_free(metadata);
    }

    // Destroy the mutexes
    pthread_mutex_destroy(&ctx->resources_mutex);
    pthread_mutex_destroy(&ctx->token_mutex);
    pthread_mutex_destroy(&ctx->blob_mutex);

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
    {FLB_CONFIG_MAP_TIME, "upload_timeout", "10m",
            0, FLB_TRUE, offsetof(struct flb_azure_kusto, upload_timeout),
    "Optionally specify a timeout for uploads. "
    "Whenever this amount of time has elapsed, Fluent Bit will complete an "
    "upload and create a new file in S3. For example, set this value to 60m "
    "and you will get a new file in S3 every hour. Default is 10m."
    },
    {FLB_CONFIG_MAP_SIZE, "upload_file_size", "500000000",
            0, FLB_TRUE, offsetof(struct flb_azure_kusto, file_size),
    "Specifies the size of files to be uploaded. Default is 500MB"
    },
    {FLB_CONFIG_MAP_TIME, "ingestion_resources_refresh_interval", FLB_AZURE_KUSTO_RESOURCES_LOAD_INTERVAL_SEC,0, FLB_TRUE,
          offsetof(struct flb_azure_kusto, ingestion_resources_refresh_interval),
          "Set the azure kusto ingestion resources refresh interval"
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
