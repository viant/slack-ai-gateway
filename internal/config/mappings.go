package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Security and integer-seconds presence are inspected before the native strict
// decoder. Only complete integration blocks contain Slack and access settings.
func validateConfigMappings(root *yaml.Node) error {
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("configuration must be a mapping")
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		key, value := root.Content[i].Value, root.Content[i+1]
		switch key {
		case "transport", "access_mode", "access", "slack", "agently", "slack_installations", "backends", "agents":
			return obsoleteAccessConfiguration()
		case "integrations":
			if value.Kind != yaml.SequenceNode {
				return fmt.Errorf("integrations must be a sequence")
			}
			for _, integration := range value.Content {
				if integration.Kind != yaml.MappingNode {
					return fmt.Errorf("integration must be a mapping")
				}
				for j := 0; j+1 < len(integration.Content); j += 2 {
					field, node := integration.Content[j].Value, integration.Content[j+1]
					switch field {
					case "access_mode":
						return obsoleteAccessConfiguration()
					case "slack":
						if err := validateSlackMapping(node); err != nil {
							return err
						}
					case "access":
						if err := validateSecurityNode(node); err != nil {
							return err
						}
						if err := validateAccessMapping(node); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

func obsoleteAccessConfiguration() error {
	return fmt.Errorf("obsolete access configuration; use integrations[].access.channels and integrations[].access.direct_messages")
}

func validateAccessMapping(node *yaml.Node) error {
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i].Value, node.Content[i+1]
		switch key {
		case "global", "defaults", "channel_overrides":
			return obsoleteAccessConfiguration()
		case "channels":
			for j := 0; j+1 < len(value.Content); j += 2 {
				field, rule := value.Content[j].Value, value.Content[j+1]
				switch field {
				case "users":
					if err := validateUserMapping(rule); err != nil {
						return err
					}
				case "channel_overrides":
					for k := 1; k < len(rule.Content); k += 2 {
						if err := validateRuleUserMapping(rule.Content[k]); err != nil {
							return err
						}
					}
				}
			}
		case "direct_messages":
			if err := validateRuleUserMapping(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRuleUserMapping(node *yaml.Node) error {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == "users" {
			if err := validateUserMapping(node.Content[i+1]); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateUserMapping(node *yaml.Node) error {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == "mode" {
			return obsoleteAccessConfiguration()
		}
	}
	return nil
}

func validateSlackMapping(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("slack must be a mapping")
	}
	for j := 0; j+1 < len(value.Content); j += 2 {
		switch value.Content[j].Value {
		case "channel_id", "channels":
			return obsoleteAccessConfiguration()
		case "status":
			node := value.Content[j+1]
			if node.Kind != yaml.MappingNode {
				return fmt.Errorf("slack.status must be a mapping")
			}
			for k := 0; k+1 < len(node.Content); k += 2 {
				if node.Content[k].Value == "mode" && node.Content[k+1].Tag != "!!str" {
					return fmt.Errorf("slack.status.mode must be message, agent_session or hybrid")
				}
				if node.Content[k].Value == "refresh_interval_sec" && node.Content[k+1].Tag != "!!int" {
					return fmt.Errorf("slack.status.refresh_interval_sec must be a positive integer number of seconds")
				}
			}
		}
	}
	return nil
}
