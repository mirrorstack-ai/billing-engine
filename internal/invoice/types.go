// Package invoice holds per-seller invoice identity, vendor provider
// accounts, and sealed vendor credentials — the piece billing-engine#312
// files, part of the invoicing layer tracked by core-v2#1850.
//
// Invoicing is a platform service, separate from every payment rail (D530):
// this package knows nothing about Stripe, NewebPay, TapPay or bank
// transfer, and the executor in internal/intent/executor is untouched by it.
// Each seller issues under its own 統編 and its own 字軌; MirrorStack
// operates the system (D533). A caller never supplies a seller directly —
// ResolveSeller derives it from a payment fact's origin and the org id
// api-platform already resolved (this engine cannot read the organizations
// schema; see the comment on billing-engine#278).
//
// Vendor credentials are sealed: this package stores only an opaque
// secret_ref (migrations/billing/093_invoice_seller_identity.up.sql) and
// never a key. See broker.go for why, and taxid.go for the 統一編號
// checksum used to validate a seller's registration.
package invoice

// SellerKind is the closed set of who a seller can be. D533/D547: the
// invoice goes FROM a seller TO that seller's customer; sellers are tenants.
type SellerKind string

const (
	// SellerKindPlatformCompany is the first-party Taiwan org
	// (mirrorstack-tw). It stays 'unregistered' until the company exists
	// (D504), so its facts end 'deferred' rather than 'applicable'.
	SellerKindPlatformCompany SellerKind = "platform_company"
	// SellerKindDistributorOrg is a distributor reselling under its own
	// 統編 and 字軌 (D535: set up in org distributor settings).
	SellerKindDistributorOrg SellerKind = "distributor_org"
	// SellerKindAppOwnerOrg is a customer app's owning org. Resolution for
	// this kind is not implemented yet — see resolve.go.
	SellerKindAppOwnerOrg SellerKind = "app_owner_org"
)

var sellerKindCatalog = map[SellerKind]struct{}{
	SellerKindPlatformCompany: {},
	SellerKindDistributorOrg:  {},
	SellerKindAppOwnerOrg:     {},
}

// SellerKindInCatalog reports whether k is a seller kind this package knows
// how to store. Mirrors the closed-catalog shape of internal/intent/catalog.go.
func SellerKindInCatalog(k SellerKind) bool {
	_, ok := sellerKindCatalog[k]
	return ok
}

// DocumentKind is a seller-level fact from the tax-bureau determination,
// never a per-sale or per-customer choice (fix direction #1).
type DocumentKind string

const (
	DocumentKindTaxInvoice DocumentKind = "tax_invoice"
	DocumentKindReceipt    DocumentKind = "receipt"
	DocumentKindNone       DocumentKind = "none"
)

// RegistrationStatus tracks whether a seller's 統編 has been issued.
type RegistrationStatus string

const (
	RegistrationUnregistered RegistrationStatus = "unregistered"
	RegistrationRegistered   RegistrationStatus = "registered"
)

// Mode is copied onto every invoice issued for a seller and is immutable
// there (fix direction #7). D504/D520: sandbox and vendor stage only until
// the Taiwan company exists; D524: a sandbox payment gets a test invoice
// only and never a production call.
type Mode string

const (
	ModeSandbox Mode = "sandbox"
	ModeLive    Mode = "live"
)

// Payer records who pays the vendor for issuing — it has no effect on what
// the seller is charged by this package (fix direction #6; the allowance and
// overage effect is billing-engine#314).
type Payer string

const (
	// PayerPlatform is the default (D544): MirrorStack pays the vendor.
	PayerPlatform Payer = "platform"
	// PayerSeller is the fallback: a seller pays its own vendor directly.
	PayerSeller Payer = "seller"
)

// Provider is the closed catalog of invoice-provider adapters (D551: e首發票
// first, ECPay second, both behind one port; 'fake' is the sandbox/dev
// adapter with no vendor call).
type Provider string

const (
	// ProviderSystemlead is e首發票 (矽聯科技 Systemlead).
	ProviderSystemlead Provider = "systemlead"
	// ProviderECPay is 綠界.
	ProviderECPay Provider = "ecpay"
	// ProviderFake makes no vendor call. Sandbox and test use only.
	ProviderFake Provider = "fake"
)

var providerCatalog = map[Provider]struct{}{
	ProviderSystemlead: {},
	ProviderECPay:      {},
	ProviderFake:       {},
}

// ProviderInCatalog reports whether p is a provider this package knows how
// to hold an account for.
func ProviderInCatalog(p Provider) bool {
	_, ok := providerCatalog[p]
	return ok
}

// ProviderCatalog returns a defensive copy of the closed provider set, for a
// test or report that needs to enumerate it.
func ProviderCatalog() []Provider {
	out := make([]Provider, 0, len(providerCatalog))
	for p := range providerCatalog {
		out = append(out, p)
	}
	return out
}

// Scope distinguishes a provider account that belongs to one seller from the
// shared platform account a seller can bind into (fix direction #2).
type Scope string

const (
	ScopeSeller   Scope = "seller"
	ScopePlatform Scope = "platform"
)

// CredentialMode is whose keys a provider account holds: the seller's own
// vendor credential, or the seller's binding into the shared platform
// credential (platform mode, for example ECPay's PlatformID plus a bound
// MerchantID). Platform mode needs no later migration — see
// migrations/billing/093_invoice_seller_identity.up.sql.
type CredentialMode string

const (
	CredentialModeSellerKeys CredentialMode = "seller_keys"
	CredentialModePlatform   CredentialMode = "platform"
)

// Environment is the vendor-side environment a provider account talks to.
// D504/D520: production is unreachable until the Taiwan company exists; that
// is enforced by what credentials ever get put, not by this type.
type Environment string

const (
	EnvironmentStage      Environment = "stage"
	EnvironmentProduction Environment = "production"
)

// KeyStatus is the lifecycle of a provider account's sealed credential.
type KeyStatus string

const (
	// KeyStatusUnset: no credential has ever been put.
	KeyStatusUnset KeyStatus = "unset"
	// KeyStatusActive: a credential is sealed and usable.
	KeyStatusActive KeyStatus = "active"
	// KeyStatusRevoked: a credential was put and then revoked. The
	// account's secret_ref may still be set, for audit trail, but is no
	// longer usable — Broker.Revoke has already been called against it.
	KeyStatusRevoked KeyStatus = "revoked"
)

// VerifyResult classifies the outcome of VerifyInvoiceCredentials: the
// adapter's cheapest read-only signed call through the broker, never a key
// read by this process.
type VerifyResult string

const (
	VerifyResultSuccess VerifyResult = "success"
	// VerifyResultFailure means the call ran and the vendor rejected the
	// credential (an authentication failure), not a network/transport error.
	VerifyResultFailure VerifyResult = "failure"
	// VerifyResultNotVerifiable means the adapter has no read-only call to
	// make for this provider (fix direction #3: "say so in the PR").
	VerifyResultNotVerifiable VerifyResult = "not_verifiable"
)

// ActorKind is who performed a credential or identity write (D525 pattern:
// the org owner enters keys; an operator may revoke but never enter or read
// one).
type ActorKind string

const (
	ActorKindOperator ActorKind = "operator"
	ActorKindOrgOwner ActorKind = "org_owner"
)

// Actor identifies who performed a verb call, for the audit table. ID is
// opaque to this package — a user id for org_owner, an operator identifier
// for operator — the same way payer/org ids are opaque elsewhere in this
// engine.
type Actor struct {
	ID   string
	Kind ActorKind
}

// Reason is a typed string rather than a bare parameter, matching this
// repo's preference (see internal/account/eligibility.Reason,
// internal/account/collection.Reason) for a reason that is always present
// rather than an optional comment.
type Reason string

// FactStatus is what GetInvoiceSellerSettings and ResolveSeller report about
// whether a seller's invoicing facts are actionable.
type FactStatus string

const (
	// FactApplicable: the seller can issue today.
	FactApplicable FactStatus = "applicable"
	// FactDeferred: the seller is meant to issue eventually but cannot yet
	// (unregistered, or registered but not configured with an active
	// provider account).
	FactDeferred FactStatus = "deferred"
	// FactNotApplicable: this seller's document_kind means no invoice is
	// ever issued (receipt or none), or the payment fact's origin needs no
	// Taiwan invoice at all (fix direction #4: the US C-corp case).
	FactNotApplicable FactStatus = "not_applicable"
)
