-- testmigrations/0001_init.sql
-- +goose Up
CREATE TABLE public.users (
  id    BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name  TEXT NOT NULL
);
-- +goose Down
DROP TABLE IF EXISTS public.users;
