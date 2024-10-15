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

#include "azure_kusto_store.h"
#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_fstore.h>
#include <fluent-bit/flb_time.h>
#include <fcntl.h>
#include <unistd.h>

/*
 * Simple and fast hashing algorithm to create keys in the local buffer
 */
static flb_sds_t gen_store_filename(const char *tag)
{
    int c;
    unsigned long hash = 5381;
    unsigned long hash2 = 5381;
    flb_sds_t hash_str;
    flb_sds_t tmp;
    struct flb_time tm;

    /* get current time */
    flb_time_get(&tm);

    /* compose hash */
    while ((c = *tag++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    hash2 = (unsigned long) hash2 * tm.tm.tv_sec * tm.tm.tv_nsec;

    /* flb_sds_printf allocs if the incoming sds is not at least 64 bytes */
    hash_str = flb_sds_create_size(64);
    if (!hash_str) {
        flb_errno();
        return NULL;
    }
    tmp = flb_sds_printf(&hash_str, "%lu-%lu", hash, hash2);
    if (!tmp) {
        flb_errno();
        flb_sds_destroy(hash_str);
        return NULL;
    }
    hash_str = tmp;

    return hash_str;
}


/* Retrieve a candidate buffer file using the tag */
struct azure_kusto_file *azure_kusto_store_file_get(struct flb_azure_kusto *ctx, const char *tag,
                                                    int tag_len)
{
    struct mk_list *head;
    struct mk_list *tmp;
    struct flb_fstore_file *fsf = NULL;
    struct azure_kusto_file *azure_kusto_file;
    int found = 0;

    /*
     * Based in the current ctx->stream_name, locate a candidate file to
     * store the incoming data using as a lookup pattern the content Tag.
     */
    mk_list_foreach_safe(head, tmp, &ctx->stream_active->files) {
        fsf = mk_list_entry(head, struct flb_fstore_file, _head);

        /* skip and warn on partially initialized chunks */
        if (fsf->data == NULL) {
            flb_plg_warn(ctx->ins, "BAD: found flb_fstore_file with NULL data reference, tag=%s, file=%s, will try to delete", tag, fsf->name);
            flb_fstore_file_delete(ctx->fs, fsf);
        }

        if (fsf->meta_size != tag_len) {
            fsf = NULL;
            continue;
        }

        /* skip locked chunks */
        azure_kusto_file = fsf->data;
        if (azure_kusto_file->locked == FLB_TRUE) {
            flb_plg_debug(ctx->ins, "File '%s' is locked, skipping", fsf->name);
            fsf = NULL;
            continue;
        }


        /* compare meta and tag */
        if (strncmp((char *) fsf->meta_buf, tag, tag_len) == 0 ) {
            flb_plg_debug(ctx->ins, "Found matching file '%s' for tag '%.*s'", fsf->name, tag_len, tag);
            found = 1;
            break;
        }
    }

    if (!found) {
        return NULL;
    } else {
        return fsf->data;
    }
}

/* Append data to a new or existing fstore file */
int azure_kusto_store_buffer_put(struct flb_azure_kusto *ctx, struct azure_kusto_file *azure_kusto_file,
                                 flb_sds_t tag, size_t tag_len,
                                 flb_sds_t data, size_t bytes) {
    int ret;
    flb_sds_t name;
    struct flb_fstore_file *fsf;
    size_t space_remaining;
    // flb_sds_t path_sds;

    if (ctx->store_dir_limit_size > 0 && ctx->current_buffer_size + bytes >= ctx->store_dir_limit_size) {
        flb_plg_error(ctx->ins, "Buffer is full: current_buffer_size=%zu, new_data=%zu, store_dir_limit_size=%zu bytes",
                      ctx->current_buffer_size, bytes, ctx->store_dir_limit_size);
        return -1;
    }

    /* If no target file was found, create a new one */
    if (azure_kusto_file == NULL) {
        name = gen_store_filename(tag);
        if (!name) {
            flb_plg_error(ctx->ins, "could not generate chunk file name");
            return -1;
        }

        flb_plg_debug(ctx->ins, "[azure_kusto] new buffer file: %s", name);

        /* Create the file */
        fsf = flb_fstore_file_create(ctx->fs, ctx->stream_active, name, bytes);
        if (!fsf) {
            flb_plg_error(ctx->ins, "could not create the file '%s' in the store",
                          name);
            flb_sds_destroy(name);
            return -1;
        }

        /* Write tag as metadata */
        ret = flb_fstore_file_meta_set(ctx->fs, fsf, (char *) tag, tag_len);
        if (ret == -1) {
            flb_plg_warn(ctx->ins, "Deleting buffer file because metadata could not be written");
            flb_fstore_file_delete(ctx->fs, fsf);
            return -1;
        }

        /* Allocate local context */
        azure_kusto_file = flb_calloc(1, sizeof(struct azure_kusto_file));
        if (!azure_kusto_file) {
            flb_errno();
            flb_plg_warn(ctx->ins, "Deleting buffer file because azure_kusto context creation failed");
            flb_fstore_file_delete(ctx->fs, fsf);
            return -1;
        }
        azure_kusto_file->fsf = fsf;
        azure_kusto_file->create_time = time(NULL);
        azure_kusto_file->size = 0; // Initialize size to 0

        /*path_sds = flb_sds_create(ctx->stream_active->path);
        if (path_sds == NULL) {
            flb_plg_error(ctx->ins, "failed to create path_sds");
            flb_free(azure_kusto_file);
            flb_fstore_file_delete(ctx->fs, fsf);
            return -1;
        }

        // Append a slash ("/") to the path
        path_sds = flb_sds_cat(path_sds, "/", 1);
        if (path_sds == NULL) {
            flb_plg_error(ctx->ins, "failed to append '/' to path_sds");
            flb_sds_destroy(path_sds);
            flb_free(azure_kusto_file);
            flb_fstore_file_delete(ctx->fs, fsf);
            return -1;
        }

        // Append the fileName to the path
        path_sds = flb_sds_cat(path_sds, azure_kusto_file->fsf->name, flb_sds_len(azure_kusto_file->fsf->name));
        if (path_sds == NULL) {
            flb_plg_error(ctx->ins, "failed to append file name to path_sds");
            flb_sds_destroy(path_sds);
            flb_free(azure_kusto_file);
            flb_fstore_file_delete(ctx->fs, fsf);
            return -1;
        }

        azure_kusto_file->file_path = path_sds;*/

        /* Use fstore opaque 'data' reference to keep our context */
        fsf->data = azure_kusto_file;
        flb_sds_destroy(name);

    }else {
        fsf = azure_kusto_file->fsf;
    }

    /* Append data to the target file */
    ret = flb_fstore_file_append(azure_kusto_file->fsf, data, bytes);

    if (ret != 0) {
        flb_plg_error(ctx->ins, "error writing data to local azure_kusto file");
        return -1;
    }

    azure_kusto_file->size += bytes;
    ctx->current_buffer_size += bytes;

    flb_plg_debug(ctx->ins, "[azure_kusto] new file size: %zu", azure_kusto_file->size);
    flb_plg_debug(ctx->ins, "[azure_kusto] current_buffer_size: %zu", ctx->current_buffer_size);

    /* if buffer is 95% full, warn user */
    if (ctx->store_dir_limit_size > 0) {
        space_remaining = ctx->store_dir_limit_size - ctx->current_buffer_size;
        if ((space_remaining * 20) < ctx->store_dir_limit_size) {
            flb_plg_warn(ctx->ins, "Buffer is almost full: current_buffer_size=%zu, store_dir_limit_size=%zu bytes",
                         ctx->current_buffer_size, ctx->store_dir_limit_size);
        }
    }
    return 0;
}

static int set_files_context(struct flb_azure_kusto *ctx)
{
    struct mk_list *head;
    struct mk_list *f_head;
    struct flb_fstore_stream *fs_stream;
    struct flb_fstore_file *fsf;
    struct azure_kusto_file *azure_kusto_file;

    mk_list_foreach(head, &ctx->fs->streams) {
        fs_stream = mk_list_entry(head, struct flb_fstore_stream, _head);

        /* skip current stream since it's new */
        if (fs_stream == ctx->stream_active) {
            continue;
        }

        /* skip multi-upload */
        if (fs_stream == ctx->stream_upload) {
            continue;
        }

        mk_list_foreach(f_head, &fs_stream->files) {
            fsf = mk_list_entry(f_head, struct flb_fstore_file, _head);
            if (fsf->data) {
                continue;
            }

            /* Allocate local context */
            azure_kusto_file = flb_calloc(1, sizeof(struct azure_kusto_file));
            if (!azure_kusto_file) {
                flb_errno();
                flb_plg_error(ctx->ins, "cannot allocate azure_kusto file context");
                continue;
            }
            azure_kusto_file->fsf = fsf;
            azure_kusto_file->create_time = time(NULL);

            /* Use fstore opaque 'data' reference to keep our context */
            fsf->data = azure_kusto_file;
        }
    }

    return 0;
}

/* Initialize filesystem storage for azure_kusto plugin */
int azure_kusto_store_init(struct flb_azure_kusto *ctx)
{
    int type;
    time_t now;
    char tmp[64];
    struct tm *tm;
    struct flb_fstore *fs;
    struct flb_fstore_stream *fs_stream;

    /* Set the storage type */
    type = FLB_FSTORE_FS;

    /* Initialize the storage context */
    if (ctx->buffer_dir[strlen(ctx->buffer_dir) - 1] == '/') {
        snprintf(tmp, sizeof(tmp), "%s%s", ctx->buffer_dir, ctx->azure_kusto_buffer_key);
    }
    else {
        snprintf(tmp, sizeof(tmp), "%s/%s", ctx->buffer_dir, ctx->azure_kusto_buffer_key);
    }

    /* Initialize the storage context */
    fs = flb_fstore_create(tmp, type);
    if (!fs) {
        return -1;
    }
    ctx->fs = fs;

    /*
     * On every start we create a new stream, this stream in the file system
     * is directory with the name using the date like '2020-10-03T13:00:02'. So
     * all the 'new' data that is generated on this process is stored there.
     *
     * Note that previous data in similar directories from previous runs is
     * considered backlog data, in the azure_kusto plugin we need to differentiate the
     * new v/s the older buffered data.
     *
     * Compose a stream name...
     */
    now = time(NULL);
    tm = localtime(&now);

#ifdef FLB_SYSTEM_WINDOWS
    /* Windows does not allow ':' in directory names */
    strftime(tmp, sizeof(tmp) - 1, "%Y-%m-%dT%H-%M-%S", tm);
#else
    strftime(tmp, sizeof(tmp) - 1, "%Y-%m-%dT%H:%M:%S", tm);
#endif

    /* Create the stream */
    fs_stream = flb_fstore_stream_create(ctx->fs, tmp);
    if (!fs_stream) {
        /* Upon exception abort */
        flb_plg_error(ctx->ins, "could not initialize active stream: %s", tmp);
        flb_fstore_destroy(fs);
        ctx->fs = NULL;
        return -1;
    }
    ctx->stream_active = fs_stream;

    set_files_context(ctx);
    return 0;
}

int azure_kusto_store_exit(struct flb_azure_kusto *ctx)
{
    struct mk_list *head;
    struct mk_list *f_head;
    struct flb_fstore_stream *fs_stream;
    struct flb_fstore_file *fsf;
    struct azure_kusto_file *azure_kusto_file;

    if (!ctx->fs) {
        return 0;
    }

    /* release local context on non-multi upload files */
    mk_list_foreach(head, &ctx->fs->streams) {
        fs_stream = mk_list_entry(head, struct flb_fstore_stream, _head);
        if (fs_stream == ctx->stream_upload) {
            continue;
        }

        mk_list_foreach(f_head, &fs_stream->files) {
            fsf = mk_list_entry(f_head, struct flb_fstore_file, _head);
            if (fsf->data != NULL) {
                azure_kusto_file = fsf->data;
                //flb_sds_destroy(azure_kusto_file->file_path);
                flb_free(azure_kusto_file);
            }
        }
    }

    if (ctx->fs) {
        flb_fstore_destroy(ctx->fs);
    }
    return 0;
}

int azure_kusto_store_has_data(struct flb_azure_kusto *ctx)
{
    struct mk_list *head;
    struct flb_fstore_stream *fs_stream;

    // Check if the file storage context is initialized
    if (!ctx->fs) {
        flb_plg_debug(ctx->ins, "File storage context is not initialized");
        return FLB_FALSE;
    }

    // Iterate over each stream in the file storage
    mk_list_foreach(head, &ctx->fs->streams) {
        fs_stream = mk_list_entry(head, struct flb_fstore_stream, _head);

        // Log the name of the current stream being processed
        flb_plg_debug(ctx->ins, "Processing stream: '%s'", fs_stream->name);

        // Check if the current stream is the one used for uploads
        if (fs_stream == ctx->stream_upload) {
            flb_plg_debug(ctx->ins, "Skipping upload stream: '%s'", fs_stream->name);
            continue;
        }

        // Log the number of files in the current stream
        int file_count = mk_list_size(&fs_stream->files);
        flb_plg_debug(ctx->ins, "Stream '%s' has %d files", fs_stream->name, file_count);

        // If there are files, log their names and return true
        if (file_count > 0) {
            struct mk_list *file_head;
            struct flb_fstore_file *fs_file;

            mk_list_foreach(file_head, &fs_stream->files) {
                fs_file = mk_list_entry(file_head, struct flb_fstore_file, _head);
                flb_plg_debug(ctx->ins, "File in stream '%s': '%s'", fs_stream->name, fs_file->name);
            }

            return FLB_TRUE;
        }
    }

    // Log if no data was found in any stream
    flb_plg_debug(ctx->ins, "No data found in any stream");
    return FLB_FALSE;
}

/*
 * Check if the store has data. This function is only used on plugin
 * initialization
 */
int azure_kusto_store_has_data_ext(struct flb_azure_kusto *ctx)
{
    struct mk_list *head;
    struct flb_fstore_stream *fs_stream;

    if (!ctx->fs) {
        return FLB_FALSE;
    }

    mk_list_foreach(head, &ctx->fs->streams) {
        /* skip multi upload stream */
        fs_stream = mk_list_entry(head, struct flb_fstore_stream, _head);
        if (fs_stream == ctx->stream_upload) {
            continue;
        }

        flb_plg_debug(ctx->ins, "Stream '%s' has %d files", fs_stream->name, mk_list_size(&fs_stream->files));

        if (mk_list_size(&fs_stream->files) > 0) {
            return FLB_TRUE;
        }
    }

    return FLB_FALSE;
}

int azure_kusto_store_has_uploads(struct flb_azure_kusto *ctx)
{
    if (!ctx || !ctx->stream_upload) {
        return FLB_FALSE;
    }

    if (mk_list_size(&ctx->stream_upload->files) > 0) {
        return FLB_TRUE;
    }

    return FLB_FALSE;
}

int azure_kusto_store_file_inactive(struct flb_azure_kusto *ctx, struct azure_kusto_file *azure_kusto_file)
{
    int ret;
    struct flb_fstore_file *fsf;

    fsf = azure_kusto_file->fsf;

    flb_free(azure_kusto_file);
    ret = flb_fstore_file_inactive(ctx->fs, fsf);

    return ret;
}

void azure_kusto_file_cleanup(struct azure_kusto_file *file)
{
    if (file == NULL) {
        return;
    }

    // Free the file path if it was dynamically allocated
    /*if (file->file_path != NULL) {
        flb_sds_destroy(file->file_path);
        file->file_path = NULL;
    }*/

    // If there are other dynamically allocated members, free them here
    // For now, we only free file_path as per the given struct

    // Free the azure_kusto_file itself
    //flb_free(file);
}

int azure_kusto_store_file_delete(struct flb_azure_kusto *ctx, struct azure_kusto_file *azure_kusto_file)
{
    struct flb_fstore_file *fsf;

    fsf = azure_kusto_file->fsf;
    ctx->current_buffer_size -= azure_kusto_file->size;

    /* permanent deletion */
    flb_fstore_file_delete(ctx->fs, fsf);
    flb_free(azure_kusto_file);

    return 0;
}

int azure_kusto_store_file_delete_ext_latest(struct flb_azure_kusto *ctx, struct azure_kusto_file *azure_kusto_file)
{
    int ret;
    struct flb_fstore_file *fsf;
    int fd;

    fsf = azure_kusto_file->fsf;
    if (fsf != NULL) {
        // Check if the file exists before attempting to open it
        if (access(azure_kusto_file->file_path, F_OK) == -1) {
            flb_plg_warn(ctx->ins, "File does not exist");
            //azure_kusto_file_cleanup(azure_kusto_file);
            //flb_free(azure_kusto_file);
            return 0;
        }

        // Open the file for locking
        fd = open(azure_kusto_file->file_path, O_RDWR);
        if (fd == -1) {
            flb_plg_error(ctx->ins, "Failed to open file for locking : because it doesnt exist");
            return 0;
        }

        // Lock the file
        if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
            if (errno == EWOULDBLOCK) {
                flb_plg_warn(ctx->ins, "File '%s' is locked by another process, skipping deletion", azure_kusto_file->file_path);
                close(fd);
                return 0;
            } else {
                flb_plg_error(ctx->ins, "Failed to lock file '%s': %s", azure_kusto_file->file_path, strerror(errno));
                close(fd);
                return -1;
            }
        }

        ctx->current_buffer_size -= azure_kusto_file->size;

        // Permanent deletion
        ret = flb_fstore_file_delete(ctx->fs, fsf);
        if (ret != 0) {
            flb_plg_error(ctx->ins, "File ingested but Failed to delete file %s with size %zu", azure_kusto_file->file_path, azure_kusto_file->size);
            ret = -1;
        } else {
            flb_plg_debug(ctx->ins, "File ingested and Deleted file '%s' and with size %zu", azure_kusto_file->file_path, azure_kusto_file->size);
            ret = 0;
        }

        flb_plg_debug(ctx->ins, "Freeing memory for azure_kusto_file at address: %p", (void *)azure_kusto_file);
        azure_kusto_file_cleanup(azure_kusto_file);
        flb_free(azure_kusto_file);
        azure_kusto_file = NULL; // Set pointer to NULL after freeing

        // Unlock the file and close the file descriptor
        flock(fd, LOCK_UN);
        close(fd);
    } else {
        flb_plg_warn(ctx->ins, "The file might have been already deleted by another process");
        ret = 0;
    }

    return ret;
}

int azure_kusto_store_file_delete_ext(struct flb_azure_kusto *ctx, struct azure_kusto_file *azure_kusto_file)
{
    int ret;
    struct flb_fstore_file *fsf;

    fsf = azure_kusto_file->fsf;
    if (fsf != NULL) {
        ctx->current_buffer_size -= azure_kusto_file->size;

        /* permanent deletion */
        ret = flb_fstore_file_delete(ctx->fs, fsf);
        // if successfully not deleted, release the lock
        if (ret != 0) {
            flb_plg_error(ctx->ins, "Failed to delete file '%s': %s", azure_kusto_file->fsf->name, strerror(errno));
            ret = -1;
        }else{
            flb_plg_debug(ctx->ins, "Deleted file '%s'", azure_kusto_file->fsf->name);
            ret = 0;
        }
        flb_plg_debug(ctx->ins, "Freeing memory for azure_kusto_file at address: %p", (void *)azure_kusto_file);
        flb_free(azure_kusto_file);
        azure_kusto_file = NULL; // Set pointer to NULL after freeing
    } else {
        flb_plg_warn(ctx->ins, "The file might have been already deleted by another process");
        ret = 0;
    }

    return ret;
}

int azure_kusto_store_file_upload_read(struct flb_azure_kusto *ctx, struct flb_fstore_file *fsf,
                                       char **out_buf, size_t *out_size)
{
    int ret;

    ret = flb_fstore_file_content_copy(ctx->fs, fsf,
                                       (void **) out_buf, out_size);
    return ret;
}

/* Always set an updated copy of metadata into the fs_store_file entry */
int azure_kusto_store_file_meta_get(struct flb_azure_kusto *ctx, struct flb_fstore_file *fsf)
{
    return flb_fstore_file_meta_get(ctx->fs, fsf);
}

void azure_kusto_store_file_lock(struct azure_kusto_file *azure_kusto_file)
{
    azure_kusto_file->locked = FLB_TRUE;
}

void azure_kusto_store_file_unlock(struct azure_kusto_file *azure_kusto_file)
{
    azure_kusto_file->locked = FLB_FALSE;
}