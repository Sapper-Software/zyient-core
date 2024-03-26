
CREATE TABLE IF NOT EXISTS test.demo_mapping_conf
(
    id character varying(255) COLLATE pg_catalog."default" NOT NULL,
    child_id character varying(255) COLLATE pg_catalog."default",
    conf_type character varying(255) COLLATE pg_catalog."default",
    filter_id character varying(255) COLLATE pg_catalog."default",
    source_path character varying(255) COLLATE pg_catalog."default",
    target_path character varying(255) COLLATE pg_catalog."default",
    name character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT demo_mapping_conf_pkey PRIMARY KEY (id),
    CONSTRAINT demo_mapping_conf_conf_type_check CHECK (conf_type::text = ANY (ARRAY['ALL'::character varying, 'PER_CASE'::character varying]::text[]))
)
