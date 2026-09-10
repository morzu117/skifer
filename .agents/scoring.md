# Scoring Policy

Score every sub-task from 1 to 5 on:

- complexity
- uncertainty
- blast_radius
- security_sensitivity
- domain_knowledge
- testability_risk
- architecture_impact
- context_size

## Default Weights

- complexity: 15%
- uncertainty: 15%
- blast_radius: 15%
- security_sensitivity: 15%
- domain_knowledge: 10%
- testability_risk: 10%
- architecture_impact: 15%
- context_size: 5%

Normalize the weighted result to a score from 0 to 100.

## Default Routing

- `0-25`: fast / economical coding model
- `26-50`: standard coding model
- `51-75`: strong coding or reasoning model
- `76-100`: maximum reasoning model

Route to strong or maximum reasoning when `security_sensitivity`,
`architecture_impact`, or `blast_radius` is `>= 4`, or when `uncertainty` is `5`.

