package domain

type RegistrationAPI interface {
	Register(request RegistrationReq) (*RegistrationResp, error)
}

type RegistrationReq struct {
}

type RegistrationResp struct {
	NodeId string
}
