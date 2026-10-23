ALTER TABLE idempotency_keys ADD COLUMN owner_id uuid;
COMMENT ON COLUMN idempotency_keys.owner_id IS 'Необязательный владелец сохранённого результата, проверенный бизнес-логикой после эффекта; не меняет scope ключа. NULL для незавершённых и не связанных с пользователем результатов.';
