import numpy as np
import logging
import scipy.stats as scistats

LOGGER = logging.getLogger(__name__)


def get_feature_count_matrix(x1, x2, u_x1, u_x2, feature_1, feature_2):
    # TODO: This won't handle cases where there are invalid values in the data
    feat_count_mat = np.zeros((len(u_x2), len(u_x1)))

    errors = False
    for cx1, cx2 in zip(x1, x2):
        if (not np.isnan(cx1)) and (not np.isnan(cx2)):
            try:
                i_x1 = np.where(u_x1 == cx1)[0][0]
                i_x2 = np.where(u_x2 == cx2)[0][0]

                feat_count_mat[i_x2, i_x1] += 1  # x2 should be on the rows and x1 on the cols
            except IndexError as e:
                errors = True
                pass
    if errors:
        LOGGER.warning(f"Computed count matrix with errors for {feature_1} and {feature_2}")

    return feat_count_mat


def get_feature_info_from_column_info(column_info):
    """Get feature qualifiers from column info."""
    if column_info['is_integer']:
        feature_qualifiers = [
            {"operator": "=", "value": i + column_info['min']}
            for i in range(column_info['range'])
        ]
    else:  # enum
        feature_qualifiers = [
            {"operator": "=", "value": k}
            for k in column_info['enum']
        ]

    return {
        'feature_name': column_info['name'],
        'feature_qualifiers': feature_qualifiers
    }


def get_feature_matrix_json(count_mat):
    """Get feature matrix from count matrix."""
    row_counts = np.sum(count_mat, axis=1)
    total = np.sum(row_counts)
    col_counts = np.sum(count_mat, axis=0)

    matrix = []
    for i in range(count_mat.shape[0]):
        row = []
        for j in range(count_mat.shape[1]):
            v = count_mat[i, j]
            row_percentage = v / row_counts[i]
            if np.isnan(row_percentage):
                row_percentage = None
            col_percentage = v / col_counts[j]
            if np.isnan(col_percentage):
                col_percentage = None
            row.append({
                "frequency": v,
                "row_percentage": row_percentage,
                "col_percentage": col_percentage,
                "total_percentage": v / total,
            })
        matrix.append(row)

    return matrix


def get_edge_stats(x1, x2, u_x1, u_x2, i_column, j_column, i_column_info, j_column_info):
    """Compute edge statistics from count matrix."""
    predicate = "biolink:correlated_with"

    count_mat = get_feature_count_matrix(x1, x2, u_x1, u_x2, i_column, j_column)
    # Currently we calculate the chi2 value and parameter
    # We can do this for any size count_mat

    eps = np.finfo(np.float32).eps
    chi_squared_statistic, chi_squared_p, chi_squared_dof, _ = scistats.chi2_contingency(count_mat + eps, correction=False)

    # we are changing some variable names for clarity
    # chi_squared ---> chi_squared_statistic
    # p_value ---> chi_squared_p

    edge_stats = {
        "chi_squared_statistic": chi_squared_statistic,
        "chi_squared_dof": chi_squared_dof,
        "chi_squared_p": chi_squared_p,
        "total_sample_size": np.sum(count_mat),
    }

    # If we only have a 2x2 count_mat and there are no zeroes we can do other tests
    if count_mat.shape == (2, 2) and not np.any(count_mat==0):

        try:
            # We could also do a fisher_exact test
            # Technically the chi_squared is an approximate at smaller sample sizes
            # At larger sample sizes this can be slow? apparently? I think it's fine though
            fisher_exact_odds_ratio, fisher_exact_p = scistats.fisher_exact(count_mat, alternative='two-sided')

            # For 2D count_mat we can also calculate the odds ratio with scipy
            # But this seems to be missing from some versions of scipy
            # oddsratio = scistats.contingency.odds_ratio(count_mat)
            # The odds_ratio method also can give you confidence internal
            # Which fisher_exact can't 

            # We can just write our own quick version though
            odds_r0 = count_mat[0][0]/count_mat[0][1]
            odds_r1 = count_mat[1][0]/count_mat[1][1]

            try:
                odds_ratio = odds_r0/odds_r1 # This is the same as fisher
                # https://www.bmj.com/content/bmj/320/7247/1468.1.full.pdf
            except Exception as e:
                LOGGER.error(f"Error comupting odds ratio: {e}")

            log_odds_ratio = np.log(odds_ratio)

            se_log_odds_ratio = np.sqrt(
                1/(count_mat[0][0] + eps) + 1/(count_mat[0][1] + eps) + 1/(count_mat[1][0] + eps) + 1/(count_mat[1][1]  + eps)
            )

            log_odds_ratio_interval = [log_odds_ratio - 1.96*se_log_odds_ratio, log_odds_ratio + 1.96*se_log_odds_ratio]

            truthy_features = [["False", "True"], ["false", "true"], ["Negative", "Positive"], ["0", "1"], ["no", "yes"]]
            # ["Ever", "Never"]
            if i_column_info["enum"] in truthy_features and j_column_info["enum"] in truthy_features:
                if log_odds_ratio < -np.log(1.25):
                    predicate = "biolink:negatively_correlated_with"
                elif log_odds_ratio > np.log(1.25):
                    predicate = "biolink:positively_correlated_with"

            edge_stats.update({
                "fisher_exact_odds_ratio": fisher_exact_odds_ratio,
                "fisher_exact_p": fisher_exact_p,
                "log_odds_ratio": log_odds_ratio,
                "log_odds_ratio_95_ci": log_odds_ratio_interval,
            })
        except Exception as e:
            LOGGER.error(f"Error computing edge stats: {e}")

    return predicate, edge_stats
