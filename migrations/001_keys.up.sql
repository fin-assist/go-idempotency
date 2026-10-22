CREATE TABLE idempotency_keys (
    scope uuid NOT NULL,
    endpoint text NOT NULL,
    key uuid NOT NULL,
    request_hash bytea NOT NULL,
    done boolean NOT NULL DEFAULT false,
    payload bytea,
    encrypted boolean NOT NULL DEFAULT false,
    reservation_id text NOT NULL DEFAULT '',
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (scope, endpoint, key)
);
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

COMMENT ON TABLE idempotency_keys IS 'Ключи идемпотентности: незавершённые резервы и сохранённые результаты запросов.';
COMMENT ON COLUMN idempotency_keys.scope IS 'Область ключа: UUID пользователя из проверенного токена; nil-UUID для методов без пользователя.';
COMMENT ON COLUMN idempotency_keys.endpoint IS 'Полное имя gRPC-метода, например /finassist.user.v1.AuthService/Register.';
COMMENT ON COLUMN idempotency_keys.key IS 'UUID логического действия клиента из metadata idempotency-key.';
COMMENT ON COLUMN idempotency_keys.request_hash IS 'SHA-256 запроса или содержимого загруженного файла; используется для обнаружения конфликта тела.';
COMMENT ON COLUMN idempotency_keys.done IS 'Признак сохранённого результата: true после завершения эффекта; false для незавершённого запроса или резерва.';
COMMENT ON COLUMN idempotency_keys.payload IS 'Сериализованный результат: protobuf-ответ с прикладными заголовками либо gRPC-статус с деталями; NULL до завершения.';
COMMENT ON COLUMN idempotency_keys.encrypted IS 'Признак шифрования всего payload алгоритмом AES-GCM с привязкой к области, методу и ключу.';
COMMENT ON COLUMN idempotency_keys.reservation_id IS 'Идентификатор владельца 15-секундного резерва для fencing; пустая строка для обычного режима и завершённых записей.';
COMMENT ON COLUMN idempotency_keys.expires_at IS 'Момент истечения срока резерва или сохранённого результата; после него ключ может быть использован заново.';
