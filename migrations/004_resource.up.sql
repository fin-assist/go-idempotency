ALTER TABLE idempotency_keys ADD COLUMN resource_type text, ADD COLUMN resource_id text;
COMMENT ON COLUMN idempotency_keys.resource_type IS 'Необязательный тип внутреннего бизнес-ресурса; задаётся сервисом, не меняет область ключа и не даёт прав доступа';
COMMENT ON COLUMN idempotency_keys.resource_id IS 'Внутренний идентификатор бизнес-ресурса для адресного поиска и удаления резервов/ответов; без ПДн и секретов';
