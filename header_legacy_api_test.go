package idempotency_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	idem "github.com/fin-assist/go-idempotency"
)

// FIN-296 from the consumer's side: an encrypted outcome stored by an older
// library version with an unsendable header is not replayed and not re-executed.
func TestAPIConsumerLegacyUnsendableHeaderReplayFailsClosed(t *testing.T) {
	for name, payload := range map[string]string{
		"non-ascii value": `{"Response":{"Body":"b2xk","Headers":{"x-result":["ё"]}}}`,
		"binary key":      `{"Response":{"Body":"b2xk","Headers":{"x-result-bin":["a"]}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, db, e, now := publicEngine(t)
			r := consumerRequest()
			// The same AES-GCM key, nonce layout and AAD (scope, operation, key) the
			// published versions used for this policy.
			cipher, err := idem.NewCipher(bytes.Repeat([]byte{42}, 32))
			if err != nil {
				t.Fatal(err)
			}
			nonce := make([]byte, cipher.NonceSize())
			if _, err = rand.Read(nonce); err != nil {
				t.Fatal(err)
			}
			aad := []byte(r.Scope + "\x00" + r.Endpoint + "\x00" + r.Key)
			sealed := cipher.Seal(nonce, nonce, []byte(payload), aad)
			expires := now.Add(time.Minute).Truncate(time.Microsecond)
			if _, err = db.pool.Exec(ctx, `INSERT INTO idempotency_keys(scope,endpoint,key,request_hash,done,payload,encrypted,expires_at) VALUES($1,$2,$3,$4,true,$5,true,$6)`, r.Scope, r.Endpoint, r.Key, r.Hash[:], sealed, expires); err != nil {
				t.Fatal(err)
			}
			calls := 0
			_, err = consumerDo(ctx, db, e, r, func(c context.Context) (idem.Response, error) {
				calls++
				return consumerEffect(db, publicOwner)(c)
			})
			if !errors.Is(err, idem.ErrStorage) || calls != 0 {
				t.Fatalf("legacy outcome replayed or re-executed: calls=%d err=%v", calls, err)
			}
			requireEffects(t, ctx, db, 0)
			var stored []byte
			var done bool
			var storedExpires time.Time
			if err = db.pool.QueryRow(ctx, `SELECT payload,done,expires_at FROM idempotency_keys WHERE key=$1`, r.Key).Scan(&stored, &done, &storedExpires); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(stored, sealed) || !done || !storedExpires.Equal(expires) {
				t.Fatalf("legacy row changed: done=%v expires=%v", done, storedExpires)
			}
		})
	}
}
