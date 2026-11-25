-- Reconstructed illustrative entity lookup, not an additional KPI graph output.
CREATE VIEW main.atlas_entites AS
SELECT entite, segment, mois_snapshot, effectif
FROM main.referentiel_entites;
