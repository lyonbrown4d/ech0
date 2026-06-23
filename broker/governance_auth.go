package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/arcgolabs/authx"
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func newConfiguredAuthEngine(cfg AuthConfig, logger *slog.Logger) *authx.Engine {
	engine := authx.NewEngine(
		authx.WithLogger(logger),
		authx.WithAuthorizer(authx.AuthorizerFunc(func(context.Context, authx.AuthorizationModel) (authx.Decision, error) {
			return authx.Decision{Allowed: true, PolicyID: authPolicyAllowAll}, nil
		})),
	)
	provider := newConfiguredAuthProvider(cfg)
	if err := engine.RegisterProvider(provider); err != nil && logger != nil {
		logger.Warn("register default auth provider failed", "error", err)
	}
	return engine
}

func newConfiguredAuthProvider(cfg AuthConfig) authx.AuthenticationProvider {
	staticProvider := newStaticTokenAuthProvider(cfg)
	fallbackProvider := newFallbackAuthProvider(cfg)
	return authx.NewSelectorAuthenticationProvider(func(
		_ context.Context,
		req AuthRequest,
	) (authx.TypedAuthenticationProvider[AuthRequest], error) {
		if strings.TrimSpace(req.Token) != "" {
			return staticProvider, nil
		}
		return fallbackProvider, nil
	})
}

func newStaticTokenAuthProvider(cfg AuthConfig) authx.TypedAuthenticationProvider[AuthRequest] {
	tokens := configuredAuthTokens(cfg.StaticTokens)
	return authx.TypedAuthenticationProviderFunc[AuthRequest](func(_ context.Context, req AuthRequest) (authx.AuthenticationResult, error) {
		token := strings.TrimSpace(req.Token)
		if identity, ok := tokens.Get(token); ok {
			identity.ClientID = nonEmpty(strings.TrimSpace(req.ClientID), identity.ClientID)
			return authenticationResult(identity), nil
		}
		return authx.AuthenticationResult{}, authx.NewError(authx.ErrorCodeUnauthenticated, "invalid auth token")
	})
}

func newFallbackAuthProvider(cfg AuthConfig) authx.TypedAuthenticationProvider[AuthRequest] {
	return authx.TypedAuthenticationProviderFunc[AuthRequest](func(_ context.Context, req AuthRequest) (authx.AuthenticationResult, error) {
		if cfg.Enabled && !cfg.AllowAnonymous {
			return authx.AuthenticationResult{}, authx.NewError(authx.ErrorCodeUnauthenticated, "auth token is required")
		}
		return authenticationResult(identityFromAuthRequest(req)), nil
	})
}

func configuredAuthTokens(tokens []StaticAuthTokenConfig) *collectionmapping.Map[string, Identity] {
	out := collectionmapping.NewMap[string, Identity]()
	for index := range tokens {
		token := tokens[index]
		tokenValue := strings.TrimSpace(token.Token)
		if tokenValue == "" {
			continue
		}
		out.Set(tokenValue, normalizeIdentity(Identity{
			Principal: token.Principal,
			Tenant:    token.Tenant,
			Namespace: token.Namespace,
			ClientID:  token.ClientID,
			Instance:  token.Instance,
		}))
	}
	return out
}

func identityFromAuthRequest(req AuthRequest) Identity {
	return normalizeIdentity(Identity{
		Principal: strings.TrimSpace(req.Principal),
		Tenant:    strings.TrimSpace(req.Tenant),
		Namespace: strings.TrimSpace(req.Namespace),
		ClientID:  strings.TrimSpace(req.ClientID),
	})
}

func authenticationResult(identity Identity) authx.AuthenticationResult {
	identity = normalizeIdentity(identity)
	return authx.AuthenticationResult{
		Principal: authPrincipal(identity),
		Details:   identityDetails(identity),
	}
}

func authPrincipal(identity Identity) authx.Principal {
	identity = normalizeIdentity(identity)
	return authx.Principal{
		ID:          identity.Principal,
		Roles:       collectionlist.NewList[string](),
		Permissions: collectionlist.NewList[string](),
		Attributes:  identityDetails(identity),
	}
}

func identityDetails(identity Identity) *collectionmapping.Map[string, any] {
	identity = normalizeIdentity(identity)
	details := collectionmapping.NewMap[string, any]()
	details.Set(authAttrTenant, identity.Tenant)
	details.Set(authAttrNamespace, identity.Namespace)
	details.Set(authAttrClientID, identity.ClientID)
	details.Set(authAttrInstance, identity.Instance)
	return details
}

func identityFromAuthResult(req AuthRequest, result authx.AuthenticationResult) Identity {
	fallback := normalizeIdentity(Identity{
		Principal: strings.TrimSpace(req.Principal),
		Tenant:    strings.TrimSpace(req.Tenant),
		Namespace: strings.TrimSpace(req.Namespace),
		ClientID:  strings.TrimSpace(req.ClientID),
	})
	identity := identityFromPrincipal(result.Principal, fallback)
	if result.Details != nil {
		identity.Tenant = stringDetail(result.Details, authAttrTenant, identity.Tenant)
		identity.Namespace = stringDetail(result.Details, authAttrNamespace, identity.Namespace)
		identity.ClientID = stringDetail(result.Details, authAttrClientID, identity.ClientID)
		identity.Instance = stringDetail(result.Details, authAttrInstance, identity.Instance)
	}
	return normalizeIdentity(identity)
}

func identityFromPrincipal(principal any, fallback Identity) Identity {
	identity := normalizeIdentity(fallback)
	switch typed := principal.(type) {
	case Identity:
		return normalizeIdentity(typed)
	case *Identity:
		if typed != nil {
			return normalizeIdentity(*typed)
		}
	}
	authPrincipal, ok := authx.PrincipalFromAny(principal)
	if !ok {
		return identity
	}
	identity.Principal = nonEmpty(authPrincipal.ID, identity.Principal)
	if authPrincipal.Attributes != nil {
		identity.Tenant = stringDetail(authPrincipal.Attributes, authAttrTenant, identity.Tenant)
		identity.Namespace = stringDetail(authPrincipal.Attributes, authAttrNamespace, identity.Namespace)
		identity.ClientID = stringDetail(authPrincipal.Attributes, authAttrClientID, identity.ClientID)
		identity.Instance = stringDetail(authPrincipal.Attributes, authAttrInstance, identity.Instance)
	}
	return normalizeIdentity(identity)
}

func authResource(resource ACLResource) string {
	return fmt.Sprintf("%s:%s/%s/%s", resource.Type, resource.Tenant, resource.Namespace, resource.Name)
}

func authContext(identity Identity, resource ACLResource) *collectionmapping.Map[string, any] {
	ctx := collectionmapping.NewMap[string, any]()
	ctx.Set(authCtxResource, string(resource.Type))
	ctx.Set(authCtxResourceID, resource.Name)
	ctx.Set(authCtxTenant, resource.Tenant)
	ctx.Set(authCtxNamespace, resource.Namespace)
	ctx.Set(authCtxClientID, identity.ClientID)
	return ctx
}

func stringDetail(details *collectionmapping.Map[string, any], key, fallback string) string {
	if details == nil {
		return fallback
	}
	value, ok := details.Get(key)
	if !ok {
		return fallback
	}
	switch typed := value.(type) {
	case string:
		return nonEmpty(strings.TrimSpace(typed), fallback)
	case fmt.Stringer:
		return nonEmpty(strings.TrimSpace(typed.String()), fallback)
	default:
		return fallback
	}
}
