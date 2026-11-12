package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Security and integer-seconds presence are inspected before the native strict
// decoder. Inline integrations reuse exactly the single-config rules.
func validateConfigMappings(root *yaml.Node) error {
	if root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		key, value := root.Content[i].Value, root.Content[i+1]
		switch key {
		case "access":
			if err := validateSecurityNode(value); err != nil {
				return err
			}
		case "slack":
			if err := validateSlackMapping(value); err != nil {
				return err
			}
		case "slack_installations", "backends", "agents":
			return fmt.Errorf("%s is obsolete; define complete slack and backend blocks inside integrations", key)
		case "integrations":
			if value.Kind != yaml.SequenceNode {
				return fmt.Errorf("%s must be a sequence", key)
			}
			for _, integration := range value.Content {
				if integration.Kind != yaml.MappingNode {
					return fmt.Errorf("integration must be a mapping")
				}
				if err := validateConfigMappings(integration); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateSlackMapping(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return nil
	}
	for j := 0; j+1 < len(value.Content); j += 2 {
		switch value.Content[j].Value {
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
		case "channels":
			if err := validateSecurityNode(value.Content[j+1]); err != nil {
				return err
			}
		}
	}
	return nil
}
