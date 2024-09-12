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

#include "azure_imds.h"
#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_sds.h>
#include <fluent-bit/flb_upstream.h>
#include <fluent-bit/flb_http_client.h>
#include <stdlib.h>
#include <string.h>

struct flb_azure_imds *flb_azure_imds_create(struct flb_config *config)
{
    struct flb_azure_imds *ctx;

    ctx = flb_malloc(sizeof(struct flb_azure_imds));
    if (!ctx) {
        flb_errno();
        return NULL;
    }

    ctx->upstream = flb_upstream_create(config, "169.254.169.254", 80, FLB_IO_TCP, NULL);
    if (!ctx->upstream) {
        flb_free(ctx);
        return NULL;
    }

    flb_stream_disable_flags(&ctx->upstream->base, FLB_IO_ASYNC);

    ctx->config = config;
    return ctx;
}

void flb_azure_imds_destroy(struct flb_azure_imds *ctx)
{
    if (ctx) {
        if (ctx->upstream) {
            flb_upstream_destroy(ctx->upstream);
        }
        flb_free(ctx);
    }
}

flb_sds_t flb_azure_imds_get_token(struct flb_azure_imds *ctx)
{
    int ret;
    flb_sds_t url;
    flb_sds_t response = NULL;
    struct flb_http_client *client;
    struct flb_connection *u_conn;
    size_t b_sent;

    url = flb_sds_create_size(256);
    if (!url) {
        flb_errno();
        return NULL;
    }

    flb_sds_printf(&url, "%s?api-version=%s&resource=%s",
                   FLB_AZURE_IMDS_ENDPOINT,
                   FLB_AZURE_IMDS_API_VERSION,
                   FLB_AZURE_IMDS_RESOURCE);

    u_conn = flb_upstream_conn_get(ctx->upstream);
    if (!u_conn) {
        flb_sds_destroy(url);
        return NULL;
    }

    flb_debug("[example] HTTP uri to be hit status: %s", url);

    client = flb_http_client(u_conn, FLB_HTTP_GET, url, NULL, 0, "169.254.169.254", 80, NULL, 0);
    flb_http_add_header(client, "Metadata", 8, "true", 4);
    flb_http_add_header(client, "client_id", 8, "8462debb-6529-4dcf-94cd-7546ff372e63", 36);

    ret = flb_http_do(client, &b_sent);
    if (ret != 0 || client->resp.status != 200) {
        flb_error("[example] connection initialization error");
        flb_error("[example] HTTP response status: %d", client->resp.status);
        flb_error("[example] HTTP response payload: %.*s", (int)client->resp.payload_size, client->resp.payload);
        flb_http_client_destroy(client);
        flb_upstream_conn_release(u_conn);
        flb_sds_destroy(url);
        return NULL;
    }

    flb_debug("[example] HTTP response status: %d", client->resp.status);
    flb_debug("[example] HTTP response payload: %.*s", (int)client->resp.payload_size, client->resp.payload);

    response = flb_sds_create_len(client->resp.payload, client->resp.payload_size);
    flb_http_client_destroy(client);
    flb_upstream_conn_release(u_conn);
    flb_sds_destroy(url);

    return response;
}
