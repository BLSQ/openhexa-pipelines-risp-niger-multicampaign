import re
from openhexa.sdk import current_run


def validate_campaign_filename(filename, valid_campaigns, valid_scales, valid_levels):
    """
    Validates if the filename follows the strict naming convention:
    Cibles_{campaign}_{year}_{campaign_scale}_{aggregation_level}.xlsx

    Args:
        filename (str): The name of the file to validate.
        valid_campaigns (list): List of valid campaign names.
        valid_scales (list): List of valid campaign scales.
        valid_levels (list): List of valid aggregation levels.

    Returns:
        (bool, dict or str): Tuple where the first element is a boolean
        indicating if the filename is valid, and the second element is either
        a dictionary of extracted metadata (if valid) or
        an error message (if invalid).
    """
    current_run.log_info(f"Validation du nom de fichier: '{filename}'")
    try:
        pattern = r"^Cibles_(?P<camp>[^_]+)_(?P<year>\d{4})_(?P<scale>.+)_(?P<level>[^_]+)\.xlsx$"
        match = re.match(pattern, filename)

        if not match:
            msg = f"Structure de nommage incorrecte: '{filename}'. Attendu: Cibles_Campagne_Année_Echelle_Niveau.xlsx"
            current_run.log_error(msg)
            raise ValueError(msg)

        campaign = match.group("camp")
        year = int(match.group("year"))
        campaign_scale = match.group("scale")
        aggregation_level = match.group("level").lower()

        if campaign not in valid_campaigns:
            msg = f"Nom du fichier '{filename}' contient une campagne non valide: '{campaign}'. Nom de campagnes valides: {', '.join(valid_campaigns)}."
            current_run.log_error(msg)
            raise ValueError(msg)

        if not (2026 <= year <= 2050):
            msg = f"Nom du fichier '{filename}' contient une année non valide: '{year}'. Années valides: 2026-2050."
            current_run.log_error(msg)
            raise ValueError(msg)

        if aggregation_level not in valid_levels:
            msg = f"Nom du fichier '{filename}' contient un niveau d'agrégation non valide: '{aggregation_level}'. Niveaux d'agrégation valides: {', '.join(valid_levels)}."
            current_run.log_error(msg)
            raise ValueError(msg)

        scales = campaign_scale.split("_")
        for s in scales:
            if s.lower() not in valid_scales:
                msg = f"Nom du fichier '{filename}' contient une échelle de campagne non valide: '{s}'. Échelles valides: {', '.join(valid_scales)}."
                current_run.log_error(msg)
                raise ValueError(msg)

        current_run.log_info(f"Nom de fichier '{filename}' est valide.")

        return True, {"campaign": campaign, "year": year, "level": aggregation_level}
    except Exception as e:
        msg = f"Erreur lors de la validation du nom de fichier '{filename}': {str(e)}"
        current_run.log_error(msg)
        raise ValueError(msg)
