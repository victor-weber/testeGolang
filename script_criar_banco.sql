CREATE TABLE public.dados_brutos
(
    cpf character varying(18) COLLATE pg_catalog."default",
    private character varying(1) COLLATE pg_catalog."default",
    incompleto character varying(1) COLLATE pg_catalog."default",
    data_ultima_compra character varying(10) COLLATE pg_catalog."default",
    ticket_medio character varying(10) COLLATE pg_catalog."default",
    ticket_ultima_compra character varying(10) COLLATE pg_catalog."default",
    loja_mais_frequente character varying(18) COLLATE pg_catalog."default",
    loja_ultima_compra character varying(18) COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE public.dados_brutos
    OWNER to postgres;


CREATE TABLE public.dados_excluidos
(
    cpf character varying(18) COLLATE pg_catalog."default",
    private character varying(1) COLLATE pg_catalog."default",
    incompleto character varying(1) COLLATE pg_catalog."default",
    data_ultima_compra character varying(10) COLLATE pg_catalog."default",
    ticket_medio character varying(10) COLLATE pg_catalog."default",
    ticket_ultima_compra character varying(10) COLLATE pg_catalog."default",
    loja_mais_frequente character varying(18) COLLATE pg_catalog."default",
    loja_ultima_compra character varying(18) COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE public.dados_excluidos
    OWNER to postgres;


CREATE TABLE public.dados_limpos
(
    cpf character varying(18) COLLATE pg_catalog."default",
    private character varying(1) COLLATE pg_catalog."default",
    incompleto character varying(1) COLLATE pg_catalog."default",
    data_ultima_compra character varying(10) COLLATE pg_catalog."default",
    ticket_medio character varying(10) COLLATE pg_catalog."default",
    ticket_ultima_compra character varying(10) COLLATE pg_catalog."default",
    loja_mais_frequente character varying(18) COLLATE pg_catalog."default",
    loja_ultima_compra character varying(18) COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE public.dados_limpos
    OWNER to postgres;