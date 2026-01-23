package orbital_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"maps"
	"testing"

	"github.com/google/uuid"
	"github.com/openkcm/common-sdk/pkg/jwtsigning"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestResponderSignatureHandler(t *testing.T) {
	t.Run("initialize the responder signature handler", func(t *testing.T) {
		// given
		tts := []struct {
			name   string
			signer *jwtsigning.Signer
			verify *jwtsigning.Verifier
			expErr error
		}{
			{
				name:   "should not return error if signer is nil",
				signer: nil,
				verify: &jwtsigning.Verifier{},
			},
			{
				name:   "should not return error if verifier is nil",
				signer: &jwtsigning.Signer{},
				verify: nil,
			},
			{
				name:   "should return error if both signer and verifier are nil",
				signer: nil,
				verify: nil,
				expErr: orbital.ErrSignerVerifierNil,
			},
			{
				name:   "should not return error if both signer and verifier are not nil",
				signer: &jwtsigning.Signer{},
				verify: &jwtsigning.Verifier{},
				expErr: nil,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// when
				result, err := orbital.NewResponderSignatureHandler(tt.signer, tt.verify)

				// then
				if tt.expErr != nil {
					assert.Nil(t, result)
				} else {
					assert.NotNil(t, result)
				}
				assert.Equal(t, tt.expErr, err)
			})
		}
	})
}

func TestInitiatorSignatureHandler(t *testing.T) {
	t.Run("initialize the initiator signature handler", func(t *testing.T) {
		// given
		tts := []struct {
			name   string
			signer *jwtsigning.Signer
			verify *jwtsigning.Verifier
			expErr error
		}{
			{
				name:   "should not return error if signer is nil",
				signer: nil,
				verify: &jwtsigning.Verifier{},
			},
			{
				name:   "should not return error if verifier is nil",
				signer: &jwtsigning.Signer{},
				verify: nil,
			},
			{
				name:   "should return error if both signer and verifier are nil",
				signer: nil,
				verify: nil,
				expErr: orbital.ErrSignerVerifierNil,
			},
			{
				name:   "should not return error if both signer and verifier are not nil",
				signer: &jwtsigning.Signer{},
				verify: &jwtsigning.Verifier{},
				expErr: nil,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// when
				result, err := orbital.NewInitiatorSignatureHandler(tt.signer, tt.verify)

				// then
				if tt.expErr != nil {
					assert.Nil(t, result)
				} else {
					assert.NotNil(t, result)
				}
				assert.Equal(t, tt.expErr, err)
			})
		}
	})
}

func TestSignAndVerify(t *testing.T) {
	t.Run("initiator signature handler ", func(t *testing.T) {
		t.Run("if signer is nil then during signing it should return empty signature and no error", func(t *testing.T) {
			// given
			subj, err := orbital.NewInitiatorSignatureHandler(nil, &jwtsigning.Verifier{})
			assert.NoError(t, err)

			// when
			result, err := subj.Sign(t.Context(), orbital.TaskRequest{})

			// then
			assert.NoError(t, err)
			assert.Empty(t, result)
		})
		t.Run("if verifier is nil then during verification it should return no error", func(t *testing.T) {
			// given
			subj, err := orbital.NewInitiatorSignatureHandler(&jwtsigning.Signer{}, nil)
			assert.NoError(t, err)

			// when
			err = subj.Verify(t.Context(), orbital.TaskResponse{})

			// then
			assert.NoError(t, err)
		})
	})
	t.Run("responder signature handler", func(t *testing.T) {
		t.Run("if signer is nil then during signing it should return empty signature and no error", func(t *testing.T) {
			// given
			subj, err := orbital.NewResponderSignatureHandler(nil, &jwtsigning.Verifier{})
			assert.NoError(t, err)

			// when
			result, err := subj.Sign(t.Context(), orbital.TaskResponse{})

			// then
			assert.NoError(t, err)
			assert.Empty(t, result)
		})

		t.Run("if verifier is nil then during verification it should return no error", func(t *testing.T) {
			// given
			subj, err := orbital.NewResponderSignatureHandler(&jwtsigning.Signer{}, nil)
			assert.NoError(t, err)

			// when
			err = subj.Verify(t.Context(), orbital.TaskRequest{})

			// then
			assert.NoError(t, err)
		})
	})

	t.Run("taskrequest", func(t *testing.T) {
		t.Run("sign and verify should be successful", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			expKid := "test-kid"
			expIss := "test-issuer"
			currentSigningKeyCalled := 0
			keyProvider.FnCurrentSigningKey = func(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
				currentSigningKeyCalled++
				return keyProvider.privateKey, jwtsigning.KeyMetadata{
					Iss: expIss,
					Kid: expKid,
				}, nil
			}

			actIss := ""
			actKid := ""
			keyProvider.FnVerificationKey = func(_ context.Context, iss string, kid string) (*rsa.PublicKey, error) {
				actIss = iss
				actKid = kid
				return &keyProvider.privateKey.PublicKey, nil
			}

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subjSigner, err := orbital.NewInitiatorSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			subjVerifier, err := orbital.NewResponderSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			req := orbital.TaskRequest{}

			// when
			sign, err := subjSigner.Sign(context.Background(), req)

			// then
			assert.NoError(t, err)
			assert.NotEmpty(t, sign)
			assert.Equal(t, 1, currentSigningKeyCalled)

			req.MetaData = make(map[string]string)
			maps.Copy(req.MetaData, sign)

			// when
			err = subjVerifier.Verify(context.Background(), req)
			// then
			assert.NoError(t, err)
			assert.Equal(t, expIss, actIss)
			assert.Equal(t, expKid, actKid)
		})

		t.Run("should return error if signing fails", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			currentSigningKeyCalled := 0
			keyProvider.FnCurrentSigningKey = func(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
				currentSigningKeyCalled++
				return nil, jwtsigning.KeyMetadata{}, assert.AnError
			}

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subj, err := orbital.NewInitiatorSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			// when
			result, err := subj.Sign(context.Background(), orbital.TaskRequest{})

			// then
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, 1, currentSigningKeyCalled)
		})

		t.Run("should return error if MetaData dont have signature during verification", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subjVerifier, err := orbital.NewResponderSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			req := orbital.TaskRequest{}

			// when
			err = subjVerifier.Verify(context.Background(), req)

			// then
			assert.ErrorIs(t, err, orbital.ErrMissingMessageSignature)
		})

		t.Run("should return error if public key is not found for the kid during verification", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			expKid := "test-kid"
			expIss := "test-issuer"
			currentSigningKeyCalled := 0
			keyProvider.FnCurrentSigningKey = func(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
				currentSigningKeyCalled++
				return keyProvider.privateKey, jwtsigning.KeyMetadata{
					Iss: expIss,
					Kid: expKid,
				}, nil
			}

			actIss := ""
			actKid := ""
			keyProvider.FnVerificationKey = func(_ context.Context, iss string, kid string) (*rsa.PublicKey, error) {
				actIss = iss
				actKid = kid
				return nil, assert.AnError
			}

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subjSigner, err := orbital.NewInitiatorSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			subjVerifier, err := orbital.NewResponderSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			req := orbital.TaskRequest{}

			// when
			sign, err := subjSigner.Sign(context.Background(), req)

			// then
			assert.NoError(t, err)
			assert.NotEmpty(t, sign)
			assert.Equal(t, 1, currentSigningKeyCalled)

			req.MetaData = make(map[string]string)
			maps.Copy(req.MetaData, sign)

			// when
			err = subjVerifier.Verify(context.Background(), req)
			// then
			assert.Error(t, err)
			assert.Equal(t, expIss, actIss)
			assert.Equal(t, expKid, actKid)
		})
	})
	t.Run("taskresponse", func(t *testing.T) {
		t.Run("sign and verify should be successful", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			expKid := "test-kid"
			expIss := "test-issuer"
			currentSigningKeyCalled := 0
			keyProvider.FnCurrentSigningKey = func(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
				currentSigningKeyCalled++
				return keyProvider.privateKey, jwtsigning.KeyMetadata{
					Iss: expIss,
					Kid: expKid,
				}, nil
			}

			actIss := ""
			actKid := ""
			keyProvider.FnVerificationKey = func(_ context.Context, iss string, kid string) (*rsa.PublicKey, error) {
				actIss = iss
				actKid = kid
				return &keyProvider.privateKey.PublicKey, nil
			}

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subjVerifier, err := orbital.NewInitiatorSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			subjSigner, err := orbital.NewResponderSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			resp := orbital.TaskResponse{}

			// when
			sign, err := subjSigner.Sign(context.Background(), resp)

			// then
			assert.NoError(t, err)
			assert.NotEmpty(t, sign)
			assert.Equal(t, 1, currentSigningKeyCalled)

			resp.MetaData = make(map[string]string)
			maps.Copy(resp.MetaData, sign)

			// when
			err = subjVerifier.Verify(context.Background(), resp)

			// then
			assert.NoError(t, err)
			assert.Equal(t, expIss, actIss)
			assert.Equal(t, expKid, actKid)
		})

		t.Run("should return error if signing fails", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			currentSigningKeyCalled := 0
			keyProvider.FnCurrentSigningKey = func(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
				currentSigningKeyCalled++
				return nil, jwtsigning.KeyMetadata{}, assert.AnError
			}

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subj, err := orbital.NewResponderSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			// when
			result, err := subj.Sign(context.Background(), orbital.TaskResponse{})

			// then
			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, 1, currentSigningKeyCalled)
		})

		t.Run("should return error if MetaData dont have signature during verification", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subjVerifier, err := orbital.NewInitiatorSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			resp := orbital.TaskResponse{}

			// when
			err = subjVerifier.Verify(context.Background(), resp)

			// then
			assert.ErrorIs(t, err, orbital.ErrMissingMessageSignature)
		})

		t.Run("should return error if public key is not found for the kid during verification", func(t *testing.T) {
			// given
			keyProvider, err := newKeyProvider()
			assert.NoError(t, err)

			expKid := "test-kid"
			expIss := "test-issuer"
			currentSigningKeyCalled := 0
			keyProvider.FnCurrentSigningKey = func(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
				currentSigningKeyCalled++
				return keyProvider.privateKey, jwtsigning.KeyMetadata{
					Iss: expIss,
					Kid: expKid,
				}, nil
			}

			actIss := ""
			actKid := ""
			keyProvider.FnVerificationKey = func(_ context.Context, iss string, kid string) (*rsa.PublicKey, error) {
				actIss = iss
				actKid = kid
				return nil, assert.AnError
			}

			signer, err := jwtsigning.NewSigner(keyProvider, nil)
			assert.NoError(t, err)

			verifier, err := jwtsigning.NewVerifier(keyProvider, nil, map[string]struct{}{
				"test-issuer": {},
			})
			assert.NoError(t, err)

			subjVerifier, err := orbital.NewInitiatorSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			subjSigner, err := orbital.NewResponderSignatureHandler(signer, verifier)
			assert.NoError(t, err)

			resp := orbital.TaskResponse{}

			// when
			sign, err := subjSigner.Sign(context.Background(), resp)

			// then
			assert.NoError(t, err)
			assert.NotEmpty(t, sign)
			assert.Equal(t, 1, currentSigningKeyCalled)

			resp.MetaData = make(map[string]string)
			maps.Copy(resp.MetaData, sign)

			// when
			err = subjVerifier.Verify(context.Background(), resp)

			// then
			assert.Error(t, err)
			assert.Equal(t, expIss, actIss)
			assert.Equal(t, expKid, actKid)
		})
	})
}

func TestCanonicalData(t *testing.T) {
	uid, err := uuid.Parse("46fafbca-2c77-4359-b52c-895d99a4778e")
	assert.NoError(t, err)
	t.Run("taskrequest should return canonical data for", func(t *testing.T) {
		// given
		tts := []struct {
			name    string
			request orbital.TaskRequest
			expData string
		}{
			{
				name:    "default task request",
				request: orbital.TaskRequest{},
				expData: "taskId:,type:,externalId:,data:,workingState:,eTag:",
			},
			{
				name: "taskID only",
				request: orbital.TaskRequest{
					TaskID: uid,
				},
				expData: "taskId:46fafbca-2c77-4359-b52c-895d99a4778e,type:,externalId:,data:,workingState:,eTag:",
			},
			{
				name: "type only",
				request: orbital.TaskRequest{
					Type: "test-type",
				},
				expData: "taskId:,type:test-type,externalId:,data:,workingState:,eTag:",
			},
			{
				name: "ExternalID only",
				request: orbital.TaskRequest{
					ExternalID: uid.String(),
				},
				expData: "taskId:,type:,externalId:46fafbca-2c77-4359-b52c-895d99a4778e,data:,workingState:,eTag:",
			},
			{
				name: "data only",
				request: orbital.TaskRequest{
					Data: []byte("test-data"),
				},
				expData: "taskId:,type:,externalId:,data:dGVzdC1kYXRh,workingState:,eTag:",
			},
			{
				name: "workingState only",
				request: orbital.TaskRequest{
					WorkingState: []byte("test-data"),
				},
				expData: "taskId:,type:,externalId:,data:,workingState:dGVzdC1kYXRh,eTag:",
			},
			{
				name: "eTag only",
				request: orbital.TaskRequest{
					ETag: "test-etag",
				},
				expData: "taskId:,type:,externalId:,data:,workingState:,eTag:test-etag",
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// when
				b, err := orbital.ToCanonicalData(tt.request)

				// then
				assert.NoError(t, err)
				assert.NotEmpty(t, b)
				actData := string(b)

				assert.Equal(t, tt.expData, actData)
			})
		}
	})
	t.Run("taskresponse should return canonical data for", func(t *testing.T) {
		// given
		tts := []struct {
			name    string
			request orbital.TaskResponse
			expData string
		}{
			{
				name:    "default task taskresponse",
				request: orbital.TaskResponse{},
				expData: "taskId:,type:,externalId:,workingState:,eTag:,status:,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "taskID only",
				request: orbital.TaskResponse{
					TaskID: uid,
				},
				expData: "taskId:46fafbca-2c77-4359-b52c-895d99a4778e,type:,externalId:,workingState:,eTag:,status:,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "type only",
				request: orbital.TaskResponse{
					Type: "type",
				},
				expData: "taskId:,type:type,externalId:,workingState:,eTag:,status:,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "externalID only",
				request: orbital.TaskResponse{
					ExternalID: uid.String(),
				},
				expData: "taskId:,type:,externalId:46fafbca-2c77-4359-b52c-895d99a4778e,workingState:,eTag:,status:,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "workingState only",
				request: orbital.TaskResponse{
					WorkingState: []byte("test-data"),
				},
				expData: "taskId:,type:,externalId:,workingState:dGVzdC1kYXRh,eTag:,status:,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "etag only",
				request: orbital.TaskResponse{
					ETag: "test-etag",
				},
				expData: "taskId:,type:,externalId:,workingState:,eTag:test-etag,status:,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "status only",
				request: orbital.TaskResponse{
					Status: "test-status",
				},
				expData: "taskId:,type:,externalId:,workingState:,eTag:,status:test-status,errorMessage:,reconcileAfterSec:0",
			},
			{
				name: "errorMessage only",
				request: orbital.TaskResponse{
					ErrorMessage: "error-message",
				},
				expData: "taskId:,type:,externalId:,workingState:,eTag:,status:,errorMessage:error-message,reconcileAfterSec:0",
			},
			{
				name: "reconcileAfterSec only",
				request: orbital.TaskResponse{
					ReconcileAfterSec: 22221231,
				},
				expData: "taskId:,type:,externalId:,workingState:,eTag:,status:,errorMessage:,reconcileAfterSec:22221231",
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// when
				b, err := orbital.ToCanonicalData(tt.request)

				// then
				assert.NoError(t, err)
				assert.NotEmpty(t, b)
				actData := string(b)

				assert.Equal(t, tt.expData, actData)
			})
		}
	})
}

type keyProvider struct {
	privateKey          *rsa.PrivateKey
	FnCurrentSigningKey func(ctx context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error)
	FnVerificationKey   func(ctx context.Context, iss string, kid string) (*rsa.PublicKey, error)
}

var _ jwtsigning.PrivateKeyProvider = &keyProvider{}

var _ jwtsigning.PublicKeyProvider = &keyProvider{}

func newKeyProvider() (*keyProvider, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, err
	}
	return &keyProvider{
		privateKey: privateKey,
	}, nil
}

// CurrentSigningKey implements [jwtsigning.PrivateKeyProvider].
func (p *keyProvider) CurrentSigningKey(ctx context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
	return p.FnCurrentSigningKey(ctx)
}

// VerificationKey implements [jwtsigning.PublicKeyProvider].
func (p *keyProvider) VerificationKey(ctx context.Context, iss string, kid string) (*rsa.PublicKey, error) {
	return p.FnVerificationKey(ctx, iss, kid)
}
