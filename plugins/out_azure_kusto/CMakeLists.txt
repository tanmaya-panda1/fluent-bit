set(src
        azure_kusto.c
        azure_kusto_conf.c
        azure_kusto_ingest.c
        azure_kusto_store.c
        azure_imds.c
)

FLB_PLUGIN(out_azure_kusto "${src}" "")