# go-idempotency

Идемпотентность для `user`, `bank-statement-data` и `assistant` ([FIN-78](https://finassist.youtrack.cloud/issue/FIN-78)). Библиотека извлекает ключ из metadata, хранит сериализованный ответ или контрактную ошибку вместе с эффектом в транзакции сервиса, воспроизводит ответ и очищает просроченные записи. Для `AskQuestion` есть отдельный короткий резерв с fencing. Источник правил — [grpc-api.md, разделы 6 и 7.4](https://github.com/fin-assist/contracts/blob/main/docs/grpc-api.md) и [архитектура](https://finassist.atlassian.net/wiki/spaces/PF/pages/5210132).

## Внедрение

### 1. Зависимость и миграции

После выпуска владельцем закрепите опубликованную версию: `go get github.com/fin-assist/go-idempotency@vX.Y.Z`. До выпуска для локальной проверки допустим `replace` на checkout библиотеки; в PR сервиса он не попадает.

В `MigrationSources()` сервиса перед собственным источником:

```go
{Name: idempotency.MigrationsName, Table: idempotency.MigrationsTable, FS: idempotency.Migrations},
{Name: "service", Table: "schema_migrations", FS: migrations.FS},
```

Примените `migrate up`. Источник `idempotency`, таблица версий `idempotency_schema_migrations`; `001_keys` создаёт `idempotency_keys` с PK `(scope, endpoint, key)` и индексом срока; `002_result_owner` добавляет nullable `owner_id`, `003_result_owner_index` — частичный индекс CONCURRENTLY для адресного удаления результатов. Примените новую миграцию до запуска обновлённой библиотеки. Down удаляет таблицу и сохранённые результаты: это операция отката схемы, не способ очистки. Валидация значений выполняется в Go; в схеме остаются типы, NOT NULL и первичный ключ. Назначение таблицы и каждого поля записано в русских COMMENT ON. Таблица правил в коде и соответствующих тестов — [docs/schema.md](docs/schema.md).

Все операции — **на primary (`-rw`), READ COMMITTED**. Передавайте `postgres.DB` через интерфейс `Tx(context.Context) (pgx.Tx, bool)`. Отсутствующая транзакция — `ErrNoTransaction`; библиотека не начинает, не коммитит и не откатывает её. На `-ro` библиотеку вызывать нельзя. Не делайте внешних вызовов внутри транзакции эффекта.

### Общий Manager и композиция библиотек (FIN-244)

`github.com/fin-assist/go-postgres v0.1.2` реализует небольшой интерфейс `Tx(ctx)` напрямую:

```go
manager := postgres.New(primaryPool) // import postgres "github.com/fin-assist/go-postgres"
engine, err := idempotency.New(manager, options)
if err != nil { return err }
publisher, err := outbox.NewPublisher(manager, time.Now)
if err != nil { return err }
guard, err := tombstone.New(manager, primaryPool, time.Now)
if err != nil { return err }
```

Сервис применяет миграции каждой библиотеки отдельным источником до этой сборки.
`primaryPool` — primary, `options` — конфигурация идемпотентности из следующего раздела.
У сервисных репозиториев, Engine, Publisher и Guard должен быть **один экземпляр Manager**.
Даже второй Manager над тем же пулом — другой владелец: его `Tx(ctx)` отвергает чужой контекст,
а `Run(ctx)` возвращает `ErrForeignTransaction`. Не подставляйте глобальный `postgres.Tx(ctx)`:
он не проверяет владельца и не подходит для привязки репозитория к конкретному пулу.

Общий путь: `engine.WithCompletion(requestCtx)` → внешний `manager.Run(completion, ...)`
→ Guard проверенного пользовательского scope **до чтения ключа**
→ `engine.Do(txCtx, request, effect)` → локальные записи/Publish через effectCtx
→ возврат ошибки Do → настоящий COMMIT. Вложенный `manager.Run` присоединяется без
savepoint и раннего commit; его ошибку нужно вернуть наружу. Внутри эффекта нет сетевых вызовов.
Guard сериализует всю пользовательскую операцию с Purge, включая чтение/расшифровку
сохранённого успеха или отказа. No-user auth отдельно проверяет tombstone владельца,
обнаруженного после проверки секрета или полученного из replay, по правилам ниже.

Отмена исходного запроса до эффекта прерывает ожидание ключа. После начала эффекта
WithCompletion позволяет закончить запись и COMMIT; отмена самого completion или его дедлайн
требует отката. Manager v0.1.2 откатывает транзакцию также при panic и runtime.Goexit.
Успешный callback ещё не означает успешную операцию: deferred FK может отказать на COMMIT,
тогда бизнес-запись, idempotency outcome и outbox event откатываются вместе.
Не возвращайте сохранённый Response/Result.Err клиенту до успешного COMMIT.

Manager сохраняет исходные ошибки БД для диагностики и обработки ограничений адаптером
сервиса. Его `MapError` не очищает произвольные ошибки и не заменяет sanitization Engine:
неизвестная ошибка эффекта по-прежнему превращается в ErrStorage с пустым Result;
неизвестная ошибка COMMIT на транспортной границе становится bare INTERNAL.
SQLSTATE + constraint сопоставляет владелец ограничения во внешнем operation-адаптере,
а не эта библиотека. Сырые SQL/Detail/payload и текст ошибки Manager клиенту не передаются.

Полная consumer-композиция проверяется в [fin244_manager_test.go](fin244_manager_test.go):
отдельное наблюдающее соединение до COMMIT, replay без повторного эффекта, откат и повтор
с тем же ключом после исправления deferred FK, callback-отказ, чужой Manager и аномальное
завершение эффекта. Для подключения достаточно импортов в тестах/примерах: production API
idempotency не зависит от конкретного Manager.

### 2. Сборка и интерцептор

```go
cipher, err := idempotency.NewCipher(refreshKey) // AES key from the service secret: 16/24/32 bytes
if err != nil { return err }
idem, err := idempotency.New(db, idempotency.Options{
    Methods: map[string]idempotency.Policy{
        "/finassist.user.v1.AuthService/StartPhoneVerification": {Encrypt: true},
        "/finassist.user.v1.AuthService/Register": {Encrypt: true},
        "/finassist.user.v1.AuthService/RefreshTokens": {TTL: 60*time.Second, Encrypt: true},
    },
    Clock: time.Now,
    Cipher: cipher,
    Registerer: registry,
    LockTimeout: time.Second,
    EffectTimeout: 30*time.Second,
})
if err != nil { return err }
```

Methods содержит стабильные идентификаторы операций этого сервиса (1–256 байт, без крайних пробелов и CR/LF/NUL). Ядро Do не зависит от транспорта: для gRPC используйте полное имя метода, для HTTP — фиксированный идентификатор или сочетание метода и шаблона маршрута, например POST /v1/actions. Путь с конкретным ID, query string или пользовательским вводом идентификатором быть не должен. gRPC-интерцептор ищет полное имя метода в этой таблице, другие методы пропускает. Политики копируются; после `New` их не меняют. Один экземпляр метрик на реестр; повторная регистрация — ошибка.

Порядок unary chain: сервисная/пользовательская идентичность → tombstone → `idem.UnaryInterceptor(scope)` → обработчик. `scope(ctx)` возвращает `user_id` **из проверенного токена**, для методов без пользовательского токена — пустую строку. При недостающей обязательной идентичности он возвращает ошибку, а не пустой scope. Непустой scope не в формате UUID — ошибка сервиса: интерцептор отвечает `INTERNAL_ERROR`, а не `VALIDATION_ERROR` по ключу клиента. Не читайте `user_id` из тела или произвольной metadata. В БД пустой scope представлен nil-UUID. Ключ — ровно одно значение `idempotency-key`, UUID; отсутствующий, повторный или неверный ключ даёт `VALIDATION_ERROR`. Регистр UUID нормализуется.

Потоковые методы (client-streaming `StatementsService/UploadStatement`) подключают `idem.StreamInterceptor(scope)` в stream chain в том же порядке: идентичность → tombstone → `StreamInterceptor` → обработчик. Правила те же, что у unary (точный allowlist `Methods`, тот же `scope`, ровно один UUID в `idempotency-key`, те же ошибки), проверка выполняется один раз при открытии потока, до чтения первого сообщения. Обработчик берёт `idem.FromContext(stream.Context())`, дочитывает поток, заполняет `Hash` через `HashUpload` и только после последнего `chunk` вызывает `Do` — сохранённый ответ, `IDEMPOTENCY_CONFLICT` и `REQUEST_IN_PROGRESS` приходят в конце потока (grpc-api.md, раздел 6). Методы вне allowlist получают исходный поток без изменений.

Синтаксическая проверка protobuf может идти до обработчика. Бизнес-проверки, лимиты и токены из тела (`refresh_token`, `registration_token`) выполняются **внутри callback**, после проверки ключа: повтор обязан получить сохранённый результат раньше этих отказов. `bank-bff` проверяет свою часть входа по контракту.

### 3. Обычный обработчик

```go
request, ok := idempotency.FromContext(ctx)
if !ok { return nil, finerr.InternalError() }
request.Hash, err = idempotency.HashProto(in)
if err != nil { return nil, finerr.InternalError() }

completion, cancel := idem.WithCompletion(ctx)
defer cancel() // AFTER db.Run has committed or rolled back
var result idempotency.Result
err = db.Run(completion, func(txCtx context.Context) error {
    var doErr error
    result, doErr = idem.Do(txCtx, request, func(effectCtx context.Context) (idempotency.Response, error) {
        // Business validation, limits and effect use repositories with effectCtx.
        out, domainErr := usecaseInTransaction(effectCtx, in)
        if domainErr != nil { return idempotency.Response{}, domainErr }
        body, marshalErr := proto.Marshal(out)
        return idempotency.Response{Body: body}, marshalErr
    })
    return doErr // NEVER return result.Err here: a cacheable error must commit.
})
if err != nil {
    if errors.Is(err, idempotency.ErrNotStored) { return nil, result.Err }
    if _, contract := finerr.ToREST(err); contract {
        var source interface { GRPCStatus() *status.Status }
        if errors.As(err, &source) { return nil, source.GRPCStatus().Err() }
    }
    return nil, finerr.InternalError()
}
if result.Err != nil { return nil, result.Err }
if err := grpc.SetHeader(ctx, metadata.MD(result.Response.Headers)); err != nil { return nil, finerr.InternalError() }
out := new(ResponseProto)
if err := proto.Unmarshal(result.Response.Body, out); err != nil { return nil, finerr.InternalError() }
return out, nil
```

Любая ошибка `Do` требует отката **всей** транзакции. `ErrNotStored` означает проверенный через `finerr.ToREST` несохраняемый контрактный исход в `result.Err`: после отката его возвращают клиенту. `result.Err` при успешном `Do` — сохраняемая ошибка: её возвращают только после успешного commit. При ошибке commit результат клиенту не возвращают как успешный. Неизвестная ошибка callback не попадает в `Result.Err`: возвращается `ErrStorage` с пустым результатом, сервис отвечает `finerr.InternalError()`; сырые ошибки SQL/сериализации наружу не передаются. Для ошибки от менеджера транзакций пример извлекает исходный gRPC status через errors.As: обычный postgres.DB.Run возвращает callback-ошибку без обёртки, но при сбое rollback может добавить служебную ошибку через errors.Join. Её текст наружу не передаётся.

`WithCompletion` нужен **до** `db.Run`: транзакция и commit получают отвязанный контекст с собственным дедлайном, по умолчанию 30 с. До эффекта `Do` и `DoReserved` работают с ключом в контексте, ограниченном и исходным запросом, и completion-дедлайном: отмена любого из них приводит к откату. После начала callback отмена клиента не мешает завершить эффект, запись ответа и commit. Библиотека не запускает фоновый обработчик; для отмены ожидания ключа используется короткий callback `context.AfterFunc`. Ограничение сервиса должно оставлять время на commit; длительность настраивается через `EffectTimeout`. Не используйте completion-контекст для внешних проверок до эффекта.

Первый запрос делает `INSERT … ON CONFLICT DO NOTHING RETURNING`; дубль ждёт на уникальном индексе. `lock_timeout` действует на операции с ключом и восстанавливается перед callback. По таймауту — `REQUEST_IN_PROGRESS` (`ABORTED`, `RetryInfo` с положительной задержкой); транзакция откатывается. Другое тело — `IDEMPOTENCY_CONFLICT` (`ALREADY_EXISTS`). Если первый запрос успел закоммититься, дубль возвращает его результат. Просроченный ключ выполняется заново; TTL готового ответа считается от его сохранения, по умолчанию 24 ч.

Для HTTP обработчик сам собирает Request из проверенного пользователя, стабильного идентификатора операции, Idempotency-Key и хеша тела, затем вызывает тот же WithCompletion → транзакция сервиса → Do. Response.Body — непрозрачные байты ответа: protobuf для gRPC или JSON для HTTP. Формат сериализации и успешный HTTP-статус определяет обработчик операции; до Do обработчик проверяет, что Idempotency-Key — ровно один UUID, и при ошибке возвращает finerr.ValidationError (400). После db.Run он отдельно обрабатывает ErrNotStored с result.Err после отката и result.Err после успешного commit; контрактные ошибки переводятся через finerr.ToREST. Пример с настоящими HTTP-запросами, JSON, replay, конфликтом тела, сохраняемой 422, несохраняемыми 404/403 и неверным ключом проверяет http_usage_test.go. Для секретных HTTP-ответов сервис явно задаёт Encrypt и соответствующий TTL в политике операции.

Для методов без пользовательского токена аккаунт может стать известен только в callback.
Возвращайте `Response{Body: body, OwnerID: verifiedUserID}` после проверки секрета:
`OwnerID` — необязательный UUID владельца результата, а не источник идентичности и не scope.
Он не меняет PK и порядок поиска: повтор Register/Refresh находит ответ в общем nil-scope,
даже когда исходный одноразовый токен уже потреблён. Пустой владелец означает NULL; nil-UUID
недопустим. Регистр нормализуется. Для пользовательского scope допустим только его же владелец.
Неверный владелец даёт `ErrInvalidRequest` и требует rollback всей транзакции.

Владелец хранится в том же UPDATE, что и ответ, и возвращается через
`result.Response.OwnerID` как после эффекта, так и при replay (Do/Reserve/DoReserved).
Копия внутри зашифрованного payload защищена AES-GCM; при replay она сверяется со столбцом.
Повреждение или подмена связи закрывает запрос с `ErrStorage`, без повторного эффекта.
Старые payload без OwnerID и строки с NULL читаются как не имеющие владельца.
Методы, обещающие владельца (Register/Refresh), при успешном replay обязаны отклонить пустой
OwnerID и проверить tombstone владельца **в транзакции до commit/отправки ответа**.
Для сохраняемых ошибок владелец необязателен; проверки авторизации остаются в сервисе.
Пример с настоящим gRPC-клиентом и отказом после удаления — `owner_usage_test.go`.

### Связь с бизнес-ресурсом (FIN-230)

Примените новые миграции **004_resource и 005_resource_index до запуска новой
библиотеки**. Они добавляют nullable resource_type/resource_id и частичный индекс
CONCURRENTLY по (scope,resource_type,resource_id). Уже выпущенные миграции не меняются.
Старые payload и строки с NULL означают отсутствие связи и читаются как прежде.

Для обычного Do эффект возвращает `Response{Body: body, Resource:
Resource{Type: "upload", ID: internalID}}`. Для внешнего эффекта, когда ресурс создаётся
до обращения к провайдеру, свяжите его с собственным резервом в той же транзакции:

```go
err := db.Run(ctx, func(ctx context.Context) error {
    var err error
    reservation, result, err = idem.Reserve(ctx, request)
    if err != nil || result.Replayed { return err }
    id, err := claimResource(ctx) // Только локальная БД, проверка доступа — сервисом.
    if err != nil { return err }
    return idem.BindResource(ctx, reservation, idempotency.Resource{
        Type: "phone_verification", ID: strconv.FormatInt(id, 10),
    })
})
// Успешный commit — затем внешний вызов, затем транзакция DoReserved.
```

BindResource проверяет scope/endpoint/key, hash, reservation_id и срок после захвата
блокировки. Повтор той же связи допустим, другая связь — ErrInvalidRequest. Потерянный,
завершённый или истёкший резерв даёт REQUEST_IN_PROGRESS без изменения связи. Ошибка
требует rollback. DoReserved наследует связь автоматически, включая разрешённый
сохраняемый отказ; явный другой Resource отклоняется. Связь и payload записываются
атомарно, при replay сверяются; для Encrypt копия защищена AES-GCM. При подмене —
ErrStorage без повторного эффекта. Do/Reserve/DoReserved возвращают Resource при replay.

Type — стабильный код 1–64 ASCII-символа a-z/0-9/._-, начинается с буквы; ID —
внутренний ID 1–256 ASCII-символов A-Z/a-z/0-9/._:-. Нулевой Resource означает отсутствие
связи, половина пары недопустима. Телефон, секрет, внешний произвольный ввод туда не
помещаются. Связь не меняет scope/OwnerID/TTL и не является источником авторизации.
Один исход связан с одним ресурсом, один ресурс может иметь несколько ключей/операций.

Первый потребитель — Start в user: свой резерв, успешный ответ и busy-отказы связаны
с ID заявки, а ключ/хеш/исходные окна восстановления остаются в phone_verification.
Новые примеры потенциального применения: загрузка выписки и ответ помощника;
подключение этих сервисов и политика очистки их ответов не входят в FIN-230.

Адресным удалением управляет сервис в своей primary-транзакции, например:

```sql
SELECT 1 FROM idempotency_keys
WHERE scope=$1::uuid AND resource_type=$2 AND resource_id=$3
FOR UPDATE NOWAIT;
DELETE FROM idempotency_keys
WHERE scope=$1::uuid AND resource_type=$2 AND resource_id=$3;
```

Перед удалением сервис берёт свой Guard ресурса/пользователя; при 55P03 откатывает
всю транзакцию, освобождая Guard, и ограниченно повторяет. Guard должен охватывать
создание/привязку и replay, иначе SELECT не защищает от ещё не видимой связи в другой
транзакции. Область задаётся явно: nil-scope целиком не очищается. Дополнительный
фильтр endpoint допустим, когда очищается одна операция. Нет FK в таблицу сервиса:
кэш может переживать бизнес-ресурс; момент удаления определяет его контракт.
Cleanup/Release удаляют метаданные со строкой, отдельная очистка не нужна.
Проверки полного gRPC-сценария успех/busy/replay/purge — resource_usage_test.go.

Старые версии библиотеки не используют новую связь. При поэтапном обновлении сервис
не должен рассчитывать на её наличие у записей, созданных старой версией; политики
проверки отсутствующей связи и перехода определяются первым потребителем.

### 4. Хеш и сохраняемые исходы

`HashProto(in)` = deterministic protobuf marshal + SHA-256, предел 4 МиБ. После изменения схемы между деплоями возможен ложный конфликт — принятое ограничение контракта. Для `RetryAnswer` хешируйте предусмотренный контрактом `answer_id`; для содержимого файла используйте `HashFile(reader, maxBytes)` до транзакции: он потоковый, превышение предела — `FILE_TOO_LARGE`; обрабатывайте ошибку чтения до эффекта.

`UploadStatement` хеширует не содержимое, а пару «содержимое + имя» по формуле контракта (grpc-api.md, раздел 6, FIN-297): `Hash = HashUpload(HashFile(чанки), header.file_name)` = `SHA-256(SHA-256(содержимое) ‖ file_name)`. Первая часть — 32 байта SHA-256 всех `chunk` в порядке потока (границы чанков не влияют), вторая — `UploadHeader.file_name` в UTF-8 как передан, без нормализации регистра, Unicode и пробелов; пустое имя ничего не добавляет. Тот же ключ и файл под другим именем — `IDEMPOTENCY_CONFLICT`, а не повтор со старым именем. `FILE_TOO_LARGE` отдаётся до конца потока (по `header.size_bytes` — до первого `chunk`) и до вызова `Do`, поэтому под ключом не сохраняется. Пример — `upload_usage_test.go` и `upload_api_test.go`.

Сохраняются байты успешного ответа (protobuf для gRPC, JSON для HTTP) либо полный gRPC status с деталями от `finerr`. Сохраняемость ошибки определяется через `finerr.ToREST`: только 400/413/422. Обычные 401/403/404/409/429/5xx, неизвестная или неполная ошибка откатывают транзакцию с ключом и эффектом. Успешный callback соответствует 2xx; HTTP-статус в библиотеку не передаётся. Максимум тела 4 МиБ, заголовков 64, значений на заголовок 64 и 16 КиБ суммарно; недопустимы `grpc-*`, ключи `-bin`, неверные имена и значения вне печатного ASCII `0x20–0x7E` (как требует gRPC для текстовой metadata; бинарные значения не переживают формат сохранённого исхода). Такой ответ даёт `ErrInvalidRequest` и откат эффекта, а не сохранённый исход, который `grpc.SetHeader` не сможет отправить ни сейчас, ни при replay ([FIN-296](https://finassist.youtrack.cloud/issue/FIN-296)). Передавайте только прикладную metadata, не транспортные заголовки; бинарные данные — в теле ответа.

**Исключения StartPhoneVerification:**

- Когда номер занят, callback возвращает `finerr.RateLimited(retryAt)` и `Response{DuplicateRetryAt: retryAt}`. Только этот метод может сохранить такой 429 до абсолютного `retry_at`. Лимит списывается выполнением, а не повтором. Обычный отказ по лимиту этого поля не ставит и не сохраняется. При replay исходный проверенный Status сохраняется целиком: обновляется только `RetryInfo.retry_delay` от сохранённого абсолютного `retry_at` и переданного `Clock`. Metadata, validation details и неизвестные `Any` не теряются. Связь метода, причины, `retry_at` и `Response.DuplicateRetryAt` проверяется повторно; противоречие или истёкшее обязательное ожидание даёт `ErrStorage` без повторного эффекта.
- Сохраняйте `expires_at` в protobuf, а `expires_in` вычисляйте после `Do` **при каждой отправке**, включая повтор: `max(0, floor(expires_at-now))`. Пример проверяет `usage_test.go`.

**Структурированные ошибки и повтор (FIN-225).** Библиотека использует опубликованный `contracts/gen/go v0.5.0`. Распознанные детали проверяет `finerr`; некорректные или дублированные детали закрываются `ErrStorage`. Неизвестные расширения `Any` сохраняются побайтно. `Status.message` остаётся машинным кодом, без текста обёрток. `RequestRetryInfo` задаёт сервис/usecase для исходного RPC: библиотека не выбирает permission по причине ошибки. Отсутствие детали остаётся отсутствием, явный `false` остаётся `false`.

Сохранённый `true` ограничивается механикой replay: время возможной отправки с учётом округления `Retry-After` должно быть **строго раньше фактического `expires_at` записи**. Равенство, истёкший или неизвестный срок не даёт положительного разрешения. У busy-429 запись истекает в `retry_at`, поэтому на replay прежний `true` становится явным `false`. Это не бизнес-запрет повторного входа, а отсутствие подтверждённого окна сохранённого исхода. Сервис переоценивает свои условия, BFF отдельно доказывает безопасность всего REST-вызова. UUID и private anchor не доказывают глобальный возраст исходного ключа. Политика TTL и сроки строк при чтении не продлеваются.

Для необязательного `RetryInfo` у сохраняемых 400/413/422 новый payload содержит private `RetryInfoUntil`: абсолютное время `Clock()+delay` при сохранении, не новый срок ключа. Пока оно впереди, выдаётся оставшаяся задержка. Когда известное ожидание прошло, деталь удаляется; явный `true` может сохраниться только до действующего `expires_at`. Старый payload без anchor не начинает прежний relative delay заново: необязательный `RetryInfo` удаляется, прежний `true` ограничивается до `false`. Непредставимая Go duration задержка даёт `ErrStorage`, а не насыщенную или минимальную длительность. Схема PostgreSQL и публичный Go API не меняются; additive anchor совместим со старым JSON-декодером и шифруется вместе с payload.

Новый library-тег после мержа публикует владелец; сервисные PR закрепляют его до использования исправленного replay. Переход consumers на новый тег и фактическая выкладка не заявляются этим PR.

Для `RefreshTokens` конструктор требует TTL ровно 60 с и `Encrypt: true`. AES-GCM шифрует весь сохранённый исход, включая заголовки; AAD связывает его со scope, методом и ключом. Храните ключ сервиса в секрете, сохраняйте прежний ключ до истечения зашифрованных записей. Этот API использует один ключ: смена ключа раньше срока сделает старые ответы нечитаемыми. Ошибка расшифровки закрывает запрос без повторного эффекта. `Register` и `StartPhoneVerification` также возвращают секреты: для них конструктор требует шифрование, TTL остаётся 24 ч. Если другие ответы содержат секреты, включите шифрование для них тоже. Библиотека не пишет ключи, ответы или SQL в логи; ошибки хранилища скрыты за `ErrStorage`.

### 5. AskQuestion: резерв перед проверкой согласия

1. В короткой транзакции сервиса вызовите `Reserve(ctx, request)`, затем commit. Если `result.Replayed`, верните результат после commit; token при этом нулевой. Ошибка требует rollback.
2. С полученным token, **вне транзакции**, проверьте согласие через `GetAssistantConsentStatus` на primary `user`. Используйте обычный контекст запроса с дедлайном unary 5 с.
3. Создайте `WithCompletion(ctx)` и в новой транзакции вызовите `DoReserved(txCtx, token, callback)`. Callback атомарно списывает лимит и создаёт вопрос/ответ; результат обрабатывается как у `Do`. Fencing проверяется перед callback и при записи ответа: совпадение `reservation_id`, срок строго больше текущего времени. Потерянный резерв не может закоммитить эффект.
4. После commit запускайте генерацию; сетевой вызов LLM не входит в транзакцию.
5. При любой ошибке до commit сначала откатите транзакцию, затем в **новой ограниченной по времени транзакции** вызовите `Release(ctx, token)`. Для очистки после отмены используйте `context.WithTimeout(context.WithoutCancel(ctx), ...)`. Это касается и отказа согласия, и лимита, и ошибки БД. Запоздалый Release удаляет только собственный незавершённый резерв. При падении процесса резерв истекает сам. Чтобы не открывать пустую cleanup-транзакцию на replay/дубле, перед `db.Run` можно проверить `token == (idempotency.Reservation{})` и пропустить очистку. Нулевой token (ошибка `Reserve` или replay) для `Release` — безопасный no-op; отложенную очистку можно ставить до вызова `Reserve` без доступа к внутренним полям token.

TTL резерва — 15 с по контракту, не TTL ответа. Дубль с тем же телом получает остаток TTL в `RetryInfo`; с другим — конфликт. После завершения — сохранённый ответ. Истёкший резерв перезанимается новой попыткой. Если при `DoReserved` другая попытка уже завершилась, возвращается её ответ; при незавершённом/просроченном/снятом резерве — `REQUEST_IN_PROGRESS` и откат. При истечении готового ответа нужна новая попытка через `Reserve`.

### Компенсация после явного внешнего отказа (FIN-229)

Если сервис до внешнего вызова резервирует локальный лимит, возвращать его после
явного отказа провайдера можно только владельцу действующей резервации:

```go
err := db.Run(cleanupCtx, func(txCtx context.Context) error {
    released, err := idem.ReleaseIfCurrent(txCtx, token)
    if err != nil { return err } // rollback
    if !released { return nil } // no ownership: no refund or request deletion
    return refundAndRemoveAttempt(txCtx)
})
```

`ReleaseIfCurrent` проверяет scope, операцию, ключ, хеш, reservation_id, незавершённость
и срок **после получения блокировки**. Возвращает `true` только после удаления своей
строки; удаление и компенсирующая запись коммитятся одной транзакцией сервиса.
Ошибка компенсации откатывает оба действия. Повтор, истёкший резерв, новая попытка,
готовый ответ и чужая область возвращают `false`, не разрешая компенсацию. Нулевой
token — `false, nil` без транзакции. Ошибка метода требует отката; результат до commit
не означает успешную компенсацию. Никакой ответ или ошибка провайдера не сохраняется.
При неопределённом внешнем исходе компенсировать нельзя: сервис восстанавливает свою
незавершённую попытку по собственному устойчивому состоянию.

Обычный `Release` сохраняет прежнюю семантику уборки, включая удаление собственного
истёкшего резерва. Его `nil` не подтверждает владение и не разрешает возврат лимита.
Новый метод не требует миграции или смены старых вызовов.

### 6. Периодическое задание и удаление пользователя

Зарегистрируйте задание сервиса (например, раз в минуту). В каждой короткой транзакции primary вызывайте `Cleanup(ctx, 1000)`, коммитьте пачку и продолжайте, пока число удалённых строк равно размеру пачки. Проверяйте отмену и завершайте задание с контекстом сервиса. Допустимый batch 1–10000, блокированные строки пропускаются через `SKIP LOCKED`. Порог — `expires_at <= Clock()`; время из часов сервиса, не SQL `now()`.

Для пользовательского scope проверка tombstone предшествует чтению ключа. Для no-user auth аккаунт известен только из результата: после Do и до commit/выдачи replay сервис проверяет непустой Response.OwnerID и tombstone. В `PurgeUserData` сервис удаляет `idempotency_keys WHERE scope=$1::uuid OR owner_id=$1::uuid` вместе с прочими данными пользователя, передавая проверенный ненулевой user_id; nil-scope целиком удалять нельзя. Сервис сериализует purge с эффектами и replay через свой tombstone guard/блокировки. Библиотека не выполняет purge сама. Cleanup удаляет владельца вместе со строкой, отдельная очистка связей не нужна.

### 7. Чек-лист нового эндпоинта

- OpenAPI требует `Idempotency-Key`, ответы 409 и проходит проверку FIN-77; RPC есть в contracts.
- Метод добавлен в точный allowlist `Methods`; scope берётся из проверенной идентичности.
- Хеш покрывает всё логическое тело по контракту; нулевой незаполненный хеш отклоняется; upload хешируется потоково, `UploadStatement` — через `HashUpload` с `file_name`; потоковый метод получает ключ через `StreamInterceptor`.
- Выбран обычный `Do` либо резерв перед внешней проверкой. Все записи идут через `Tx(ctx)` на primary, транзакцией владеет сервис.
- Ошибки маппятся через `finerr`; сохраняемая ошибка коммитится, несохраняемая откатывается.
- `WithCompletion` окружает транзакцию и commit, а не только callback; внешних вызовов внутри callback нет.
- Секреты зашифрованы; TTL настроен; очистка и purge подключены; заголовки прикладные и ограничены.
- Сервисные API-тесты проверяют повтор, конфликт, параллельность, отмену, чужой scope, tombstone и ошибки метода.

## Метрики

`finassist_idempotency_requests_total{endpoint,outcome}`: `new`, `replay`, `conflict`, `in_progress`. `endpoint` — только операция из конфигурации; пользователь и ключ в метки не попадают. Счётчики фиксируют решение библиотеки до внешнего commit, поэтому не являются числом закоммиченных эффектов. Отслеживайте рост конфликтов/ожиданий и ошибки задания очистки; пороги задаёт сервис.

## Раскладка и проверки

`types.go`, `context.go`, `engine.go`, `storage.go`, `codec.go` — API, metadata/хеш, выполнение, PostgreSQL и сохранённые исходы. `migrations/` — embedded источник; `internal/pgtest/` — тестовая БД и up → down → up. `*_test.go` — unit, реальный PostgreSQL и usage gRPC/HTTP; `example_test.go` — компилируемые примеры. Правила — [AGENTS.md](AGENTS.md), версии — [docs/releasing.md](docs/releasing.md), изменения — [CHANGELOG.md](CHANGELOG.md).

```sh
make test  # PostgreSQL 18 via Docker, or TEST_DATABASE_URL; vet, race, scripts, changelog
make lint
make cover # three independent native statements gates
make api   # exported API versus latest tag
```

Все проверки в окружении агента запускаются с ограничением времени процесса. Без Docker/`TEST_DATABASE_URL` интеграционные тесты локально пропускаются, а в CI падают; пропуск не подтверждает работу с БД. CI также проверяет неизменность прежних миграций и squawk для новых. Все три уровня независимо требуют 80 % statements; сценарии важнее процента.

### Независимое покрытие statements (FIN-243)

Команды и точная область инструментирования — [docs/testing.md](docs/testing.md).

`make cover-unit`, `make cover-integration` и `make cover-api` независимо требуют **80 % statements** production-кода всех пакетов `./...`, кроме единственного согласованного тестового helper `internal/pgtest`. Unit работает без сети и БД; integration проверяет реальные зависимости; API использует публичный Go API из внешнего тестового пакета и caller-owned PostgreSQL-транзакции. Существующие внутренние usage-тесты остаются integration.

Коллекторы используют одинаковый список `-coverpkg`: штатный `go list -race ./...` перечисляет пакеты, исключается только точное имя `<module>/internal/pgtest`; одноимённые соседние пакеты остаются. Suite по-прежнему запускает `./...`, включая тесты helper. Analyzer не скрывает файлы. Коллекторы используют `-race -count=1 -timeout=8m` и селекторы `TestUnit`/`FuzzUnit`, `TestIntegration`/`FuzzIntegration`, `TestAPI`/`FuzzAPI`; исполняемые `Example` входят в API (обычные примеры без Output только компилируются). `make test-names` проверяет race inventory перед каждым коллектором. JSON execution guard требует реально выполненные успешные Test и отклоняет fail/skip; profile guard отклоняет отсутствующий, пустой, header-only и zero-statement профиль. Обычные пакеты без тестов допустимы. `CI=true` делает недоступную БД ошибкой.

Профили собираются Go из `go.mod`. Только анализатор запускается с `GOTOOLCHAIN=auto`, поскольку требует Go1.27+, даже когда `setup-go` задаёт local; закреплён `go-test-coverage/v2 v2.20.1`; принято его округление **79,95 % → 80,0 %**. `make cover` собирает три профиля и проверяет каждый, `make cover-combined` строит только информационное объединение встроенным comma-profile механизмом инструмента. Низкий уровень нельзя компенсировать combined. Профили `unit.out`, `integration.out`, `api.out`, JSON и штатные отчёты сохраняются как artifacts и в summary. Required check `go` падает при ошибке любого suite/gate; PR-write права и комментарии coverage не нужны.

`make test` сохраняет race-тесты всех уровней, vet, Python-тесты скриптов и CHANGELOG; `make lint`, `make api` и release/migration/Renovate checks сохраняются. Все тестовые запуски ограничиваются временем процесса. БД локально: Docker PostgreSQL18 или `TEST_DATABASE_URL`; пропуск не является успешной проверкой.


## Автоматизация обновлений зависимостей

[Политика Renovate](docs/renovate-policy.md) (FIN-211): одна актуальная release/main, автомерж полностью ботских PR после обязательного CI, один повтор красного CI и закрытие с явным повтором через Dependency Dashboard. AndroidX/Compose остаются на ручной проверке. Исходные группировки, ограничения версий и расписания — `renovate-base.json`; generated target и quarantine — ветка `renovate-policy`. Активация требует отдельного токена и строгой защиты веток; по умолчанию выключена.
