package providers

import (
	"context"
)

type Skill struct {
	Name        string
	Description string
}

type SkillProvider interface {
	ListSkills(context.Context) ([]Skill, error)
	StartSession(context.Context, string) (string, error)
}
