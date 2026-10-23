# Схема идемпотентности

Источник миграций — `idempotency`, таблица версий — `idempotency_schema_migrations`. Миграция `001_keys` создаёт `idempotency_keys` и индекс по `expires_at`; `002_result_owner` добавляет nullable `owner_id`, `003_result_owner_index` — частичный индекс CONCURRENTLY; таблица и все поля описаны русскими `COMMENT ON`.

В DDL остаются типы, `NOT NULL`, `DEFAULT` и `PRIMARY KEY (scope, endpoint, key)`. Первичный ключ нужен для атомарного захвата и ожидания дубля. Проверки значений и состояний выполняются кодом библиотеки в транзакции сервиса на primary; `CHECK`, триггеров и хранимых функций нет.

`endpoint` — стабильный идентификатор операции, независимый от транспорта. gRPC-адаптер использует полное имя метода; HTTP-обработчик может использовать фиксированное имя либо метод и шаблон маршрута. Конкретные пользовательские ID и query string в идентификатор не входят.

## Инварианты в коде сервиса

Библиотека получает транзакцию через `Tx(ctx)`; её начинает и завершает сервис через `db.Run`.

| Правило | Где обеспечивается | Проверка |
| --- | --- | --- |
| Операция непустая, не длиннее 256 байт, без крайних пробелов и CR/LF/NUL; запрос использует настроенную операцию | `New`, `Engine.validate` в `engine.go` | `TestMethodLengthValidatedBeforeDatabase`, `TestOperationIDsAreIndependentOfTransport`, `TestUsageHTTPHandlerUsesTransportIndependentDo` |
| Область и ключ имеют формат UUID; пустая область — nil-UUID, регистр нормализуется; другая область не получает чужой ответ | `UnaryInterceptor`, `Engine.validate`, запросы по полному PK | `TestHashAndMetadata`, `TestPostgresReplayConflictScopeExpiryRollback`, `TestUsageAskQuestionReservationConsentReleaseAndScopes` |
| Хеш записывается как 32 байта и не бывает незаполненным; хеш неверной длины из БД отклоняется до эффекта | `Request.Hash [32]byte`, `Engine.validate`, `readRecord` | `TestZeroHashRejected`, `TestPostgresInvalidStoredHashRejectedByCode` |
| Тело ответа не больше 4 МиБ, заголовки ограничены по числу и размеру; payload не больше 6 000 000 байт | `validResponse`, `encode`, `decode` | `TestCodecPoliciesAndEncryption`, `TestCodecInvalidOutcomesFailClosed` |
| `done=true` записывается вместе с сериализованным payload; несохраняемый исход откатывает ключ и эффект | `Engine.perform`; ошибка `Do` возвращается из callback `db.Run` | `TestPostgresReplayConflictScopeExpiryRollback`, `TestPostgresErrorsAndRateLimitedException` |
| Другой хеш не повторяет эффект; одновременно выполняется только владелец ключа | PK, `claim`, `Engine.existing`; блокировки в транзакции | `TestPostgresConcurrentTimeoutAndWaitForCommit`, `TestPostgresWaitingDuplicateAcquiresAfterOwnerRollback`, HTTP- и gRPC-usage-тесты |
| Просроченная запись не воспроизводится; срок определяется часами сервиса | `claim`, `DoReserved`, `Cleanup`, сравнение с `Clock()` | `TestPostgresReplayConflictScopeExpiryRollback`, `TestPostgresCleanupSkipsLockedAndExpiredSavedReservationNeedsNewReserve` |
| Резерв закрепляет только владелец до истечения срока; запоздалое снятие не удаляет чужую попытку или готовый ответ | `DoReserved`, условие финального `UPDATE`, `Release` | `TestPostgresReservationFencingReleaseAndReplay`, `TestUsageAskQuestionReservationConsentReleaseAndScopes` |
| Секретные ответы шифруются AES-GCM с AAD области, операции и ключа; повреждённый payload не запускает эффект заново | Политики сервиса, `New`, `encode`, `decode` | `TestNewValidation`, `TestCodecPoliciesAndEncryption`, `TestPostgresStorageFailureAndEncryptedCorruptionDoNotRunEffect` |

## Владелец результата (FIN-226)

`Response.OwnerID` задаётся эффектом после проверки секрета/поиска аккаунта. Это не scope
и не аутентификация: nil-scope Register/Refresh сохраняется. Пустая строка становится NULL;
непустой UUID нормализуется, nil-UUID и несовпадение с пользовательским scope отклоняются
в `encode` (`ErrInvalidRequest`, rollback). `perform` сохраняет owner_id и payload одним
UPDATE. Владелец также внутри payload; `decode` проверяет UUID, scope и совпадение со столбцом,
включая отсутствие владельца. Для шифрованного результата эта копия защищена AES-GCM.
Несогласованность — `ErrStorage` без повторного эффекта. Do, Reserve и DoReserved возвращают
одного владельца через `Result.Response.OwnerID`. Незавершённые/старые записи имеют NULL.
FK на таблицы сервиса нет: связь должна пережить удаление аккаунта до purge ключей.

Проверки: `TestCodecOwnerValidationAndBinding`, `TestPostgresOwnerAtomicReplayPurgeAndCleanup`,
`TestPostgresOwnerInvalidAndStorageFailureRollBack`, `TestPostgresReservedOwnerReplaysThroughEveryEntryPoint`,
`TestUsageNoUserOwnerGuardsCachedSecrets`. Старый формат payload проверяется явно; `TestPostgresUpgradeKeepsLegacyOwnerlessReplay` применяет новые миграции к живой схеме v0.1.0 с сохранённым ответом.

## Удаление записей

Сервис регистрирует периодическое задание, вызывающее `Cleanup` в коротких primary-транзакциях. За вызов удаляется не больше заданной пачки просроченных строк; заблокированные строки пропускаются через `SKIP LOCKED`. Обычный TTL — 24 часа, для refresh — 60 секунд; незавершённый резерв живёт 15 секунд. TTL сам по себе не запускает фоновый процесс: физическая очистка происходит при проходе задания либо перезахвате просроченного ключа.

При удалении пользователя сервис удаляет строки `WHERE scope=$1::uuid OR owner_id=$1::uuid` в `PurgeUserData`, передавая проверенный ненулевой user_id. Nil-UUID методов без пользователя целиком не очищается. Tombstone и блокировки сервиса сериализуют удаление с эффектами/выдачей replay; индекс и поле владельца сами этого не обеспечивают. Cleanup удаляет строку целиком вместе с владельцем: осиротевшие связи невозможны. Проверки очистки — `TestPostgresEncryptionCleanupAndLockSetting` и `TestPostgresCleanupSkipsLockedAndExpiredSavedReservationNeedsNewReserve`.

## Применение owner-миграций

Добавление nullable-столбца совместимо со старым кодом; удаляемых/переименованных
столбцов нет, поэтому двух выпусков expand/contract не требуется. Перед запуском
нового кода сервис применяет миграции 002/003. Индекс строится CONCURRENTLY отдельным
файлом, без блокировки записей на всё время построения. Если построение прервано,
golang-migrate оставляет dirty; оператор проверяет pg_index.indisvalid, удаляет
незавершённый индекс и только затем восстанавливает версию/повторяет миграцию.
IF NOT EXISTS намеренно отсутствует: он мог бы молча принять невалидный индекс.
