"""
opsense.ml - Classical machine learning functions.

Provides scikit-learn based ML functions for regression, classification,
clustering, anomaly detection, and dimensionality reduction.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, IsolationForest, GradientBoostingRegressor
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from sklearn.decomposition import PCA, FastICA
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, accuracy_score, silhouette_score
from sklearn.pipeline import Pipeline
import warnings
warnings.filterwarnings("ignore")


def _prepare_features_target(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    dropna: bool = True,
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """Prepare feature matrix X and target vector y from DataFrame."""
    if target not in df.columns:
        raise ValueError(f"Target column '{target}' not found")
    
    if features is None:
        feature_cols = [c for c in df.columns if c != target and pd.api.types.is_numeric_dtype(df[c])]
    else:
        feature_cols = features
        for f in features:
            if f not in df.columns:
                raise ValueError(f"Feature column '{f}' not found")
    
    data = df[feature_cols + [target]].copy()
    if dropna:
        data = data.dropna()
    
    if len(data) == 0:
        raise ValueError("No data after dropping NaN")
    
    X = data[feature_cols].values
    y = data[target].values
    
    return X, y, feature_cols


def linear_regression(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    fit_intercept: bool = True,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Dict[str, Any]:
    """
    Linear regression with automatic train/test split.
    
    Args:
        df: DataFrame with features and target
        target: Target column name
        features: Feature column names (default: all numeric except target)
        fit_intercept: Whether to fit intercept
        test_size: Test set fraction
        random_state: Random seed
    
    Returns:
        Dict with coefficients, metrics, predictions
    """
    X, y, feature_names = _prepare_features_target(df, target, features)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    model = LinearRegression(fit_intercept=fit_intercept)
    model.fit(X_train, y_train)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    return {
        "model": "LinearRegression",
        "coefficients": dict(zip(feature_names, model.coef_.tolist())),
        "intercept": float(model.intercept_) if fit_intercept else 0.0,
        "train_r2": float(r2_score(y_train, y_pred_train)),
        "test_r2": float(r2_score(y_test, y_pred_test)),
        "train_mse": float(mean_squared_error(y_train, y_pred_train)),
        "test_mse": float(mean_squared_error(y_test, y_pred_test)),
        "train_mae": float(mean_absolute_error(y_train, y_pred_train)),
        "test_mae": float(mean_absolute_error(y_test, y_pred_test)),
        "feature_names": feature_names,
        "n_train": len(X_train),
        "n_test": len(X_test),
    }


def ridge_regression(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Dict[str, Any]:
    """Ridge regression (L2 regularization)."""
    X, y, feature_names = _prepare_features_target(df, target, features)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    model = Ridge(alpha=alpha, random_state=random_state)
    model.fit(X_train, y_train)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    return {
        "model": "Ridge",
        "alpha": alpha,
        "coefficients": dict(zip(feature_names, model.coef_.tolist())),
        "intercept": float(model.intercept_),
        "train_r2": float(r2_score(y_train, y_pred_train)),
        "test_r2": float(r2_score(y_test, y_pred_test)),
        "train_mse": float(mean_squared_error(y_train, y_pred_train)),
        "test_mse": float(mean_squared_error(y_test, y_pred_test)),
        "feature_names": feature_names,
    }


def lasso_regression(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    alpha: float = 1.0,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Dict[str, Any]:
    """Lasso regression (L1 regularization) - performs feature selection."""
    X, y, feature_names = _prepare_features_target(df, target, features)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    model = Lasso(alpha=alpha, random_state=random_state, max_iter=10000)
    model.fit(X_train, y_train)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    # Feature selection - non-zero coefficients
    selected_features = [f for f, c in zip(feature_names, model.coef_) if abs(c) > 1e-10]
    
    return {
        "model": "Lasso",
        "alpha": alpha,
        "coefficients": dict(zip(feature_names, model.coef_.tolist())),
        "intercept": float(model.intercept_),
        "selected_features": selected_features,
        "n_selected": len(selected_features),
        "train_r2": float(r2_score(y_train, y_pred_train)),
        "test_r2": float(r2_score(y_test, y_pred_test)),
        "train_mse": float(mean_squared_error(y_train, y_pred_train)),
        "test_mse": float(mean_squared_error(y_test, y_pred_test)),
        "feature_names": feature_names,
    }


def elastic_net(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Dict[str, Any]:
    """Elastic Net (L1 + L2 regularization)."""
    X, y, feature_names = _prepare_features_target(df, target, features)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    model = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=random_state, max_iter=10000)
    model.fit(X_train, y_train)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    selected_features = [f for f, c in zip(feature_names, model.coef_) if abs(c) > 1e-10]
    
    return {
        "model": "ElasticNet",
        "alpha": alpha,
        "l1_ratio": l1_ratio,
        "coefficients": dict(zip(feature_names, model.coef_.tolist())),
        "intercept": float(model.intercept_),
        "selected_features": selected_features,
        "train_r2": float(r2_score(y_train, y_pred_train)),
        "test_r2": float(r2_score(y_test, y_pred_test)),
        "train_mse": float(mean_squared_error(y_train, y_pred_train)),
        "test_mse": float(mean_squared_error(y_test, y_pred_test)),
        "feature_names": feature_names,
    }


def random_forest_regressor(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    n_estimators: int = 100,
    max_depth: Optional[int] = None,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Dict[str, Any]:
    """Random Forest regression with feature importance."""
    X, y, feature_names = _prepare_features_target(df, target, features)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    model = RandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    importances = dict(zip(feature_names, model.feature_importances_.tolist()))
    sorted_importances = dict(sorted(importances.items(), key=lambda x: x[1], reverse=True))
    
    return {
        "model": "RandomForestRegressor",
        "n_estimators": n_estimators,
        "feature_importances": sorted_importances,
        "train_r2": float(r2_score(y_train, y_pred_train)),
        "test_r2": float(r2_score(y_test, y_pred_test)),
        "train_mse": float(mean_squared_error(y_train, y_pred_train)),
        "test_mse": float(mean_squared_error(y_test, y_pred_test)),
        "feature_names": feature_names,
    }


def gradient_boosting_regressor(
    df: pd.DataFrame,
    target: str,
    features: Optional[List[str]] = None,
    n_estimators: int = 100,
    learning_rate: float = 0.1,
    max_depth: int = 3,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Dict[str, Any]:
    """Gradient Boosting regression."""
    X, y, feature_names = _prepare_features_target(df, target, features)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    model = GradientBoostingRegressor(
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        max_depth=max_depth,
        random_state=random_state,
    )
    model.fit(X_train, y_train)
    
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    importances = dict(zip(feature_names, model.feature_importances_.tolist()))
    
    return {
        "model": "GradientBoostingRegressor",
        "n_estimators": n_estimators,
        "learning_rate": learning_rate,
        "feature_importances": dict(sorted(importances.items(), key=lambda x: x[1], reverse=True)),
        "train_r2": float(r2_score(y_train, y_pred_train)),
        "test_r2": float(r2_score(y_test, y_pred_test)),
        "train_mse": float(mean_squared_error(y_train, y_pred_train)),
        "test_mse": float(mean_squared_error(y_test, y_pred_test)),
        "feature_names": feature_names,
    }


def isolation_forest(
    df: pd.DataFrame,
    features: Optional[List[str]] = None,
    contamination: float = 0.01,
    n_estimators: int = 100,
    random_state: int = 42,
) -> Dict[str, Any]:
    """
    Isolation Forest for anomaly detection.
    
    Args:
        df: DataFrame with features
        features: Feature columns (default: all numeric)
        contamination: Expected proportion of anomalies
        n_estimators: Number of trees
    
    Returns:
        Dict with anomaly scores, labels, feature importances
    """
    if features is None:
        feature_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    else:
        feature_cols = features
    
    data = df[feature_cols].dropna()
    if len(data) == 0:
        raise ValueError("No data after dropping NaN")
    
    X = data.values
    
    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1,
    )
    
    anomaly_scores = model.fit(X).decision_function(X)
    predictions = model.predict(X)  # -1 = anomaly, 1 = normal
    
    # Anomaly score (higher = more anomalous)
    # decision_function: lower = more anomalous, so invert
    anomaly_score = -anomaly_scores
    
    # Feature importance for Isolation Forest (based on depth)
    importances = dict(zip(feature_cols, model.feature_importances_.tolist()))
    
    result_df = df.loc[data.index].copy()
    result_df["anomaly_score"] = anomaly_score
    result_df["is_anomaly"] = predictions == -1
    
    return {
        "model": "IsolationForest",
        "contamination": contamination,
        "n_estimators": n_estimators,
        "anomaly_count": int((predictions == -1).sum()),
        "anomaly_ratio": float((predictions == -1).mean()),
        "feature_importances": dict(sorted(importances.items(), key=lambda x: x[1], reverse=True)),
        "results": result_df,
    }


def kmeans_clustering(
    df: pd.DataFrame,
    features: Optional[List[str]] = None,
    n_clusters: int = 3,
    scaler: str = "standard",
    random_state: int = 42,
) -> Dict[str, Any]:
    """
    K-Means clustering.
    
    Args:
        df: DataFrame with features
        features: Feature columns
        n_clusters: Number of clusters
        scaler: "standard", "minmax", "robust", or None
    
    Returns:
        Dict with cluster labels, centroids, silhouette score
    """
    if features is None:
        feature_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    else:
        feature_cols = features
    
    data = df[feature_cols].dropna()
    if len(data) == 0:
        raise ValueError("No data after dropping NaN")
    
    X = data.values
    
    # Scale
    scaler_map = {
        "standard": StandardScaler(),
        "minmax": MinMaxScaler(),
        "robust": RobustScaler(),
        None: None,
    }
    
    if scaler and scaler in scaler_map:
        scaler_obj = scaler_map[scaler]
        X_scaled = scaler_obj.fit_transform(X)
    else:
        X_scaled = X
        scaler_obj = None
    
    model = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
    labels = model.fit_predict(X_scaled)
    
    # Silhouette score
    sil_score = silhouette_score(X_scaled, labels) if len(set(labels)) > 1 else -1
    
    # Centroids (in original scale if scaled)
    if scaler_obj:
        centroids = scaler_obj.inverse_transform(model.cluster_centers_)
    else:
        centroids = model.cluster_centers_
    
    centroids_df = pd.DataFrame(centroids, columns=feature_cols)
    
    result_df = df.loc[data.index].copy()
    result_df["cluster"] = labels
    
    return {
        "model": "KMeans",
        "n_clusters": n_clusters,
        "labels": labels.tolist(),
        "centroids": centroids_df.to_dict("records"),
        "silhouette_score": float(sil_score),
        "inertia": float(model.inertia_),
        "feature_names": feature_cols,
        "scaler": scaler,
        "results": result_df,
    }


def dbscan_clustering(
    df: pd.DataFrame,
    features: Optional[List[str]] = None,
    eps: float = 0.5,
    min_samples: int = 5,
    scaler: str = "standard",
) -> Dict[str, Any]:
    """DBSCAN clustering."""
    if features is None:
        feature_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    else:
        feature_cols = features
    
    data = df[feature_cols].dropna()
    X = data.values
    
    scaler_map = {
        "standard": StandardScaler(),
        "minmax": MinMaxScaler(),
        "robust": RobustScaler(),
        None: None,
    }
    
    if scaler and scaler in scaler_map:
        X_scaled = scaler_map[scaler].fit_transform(X)
    else:
        X_scaled = X
    
    model = DBSCAN(eps=eps, min_samples=min_samples, n_jobs=-1)
    labels = model.fit_predict(X_scaled)
    
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = (labels == -1).sum()
    
    sil_score = silhouette_score(X_scaled, labels) if n_clusters > 1 else -1
    
    result_df = df.loc[data.index].copy()
    result_df["cluster"] = labels
    
    return {
        "model": "DBSCAN",
        "eps": eps,
        "min_samples": min_samples,
        "n_clusters": n_clusters,
        "n_noise": int(n_noise),
        "labels": labels.tolist(),
        "silhouette_score": float(sil_score),
        "feature_names": feature_cols,
        "results": result_df,
    }


def pca_analysis(
    df: pd.DataFrame,
    features: Optional[List[str]] = None,
    n_components: Optional[int] = None,
    scaler: str = "standard",
    variance_threshold: float = 0.95,
) -> Dict[str, Any]:
    """
    Principal Component Analysis.
    
    Args:
        df: DataFrame with features
        features: Feature columns
        n_components: Number of components (auto if None based on variance_threshold)
        scaler: Scaling method
        variance_threshold: Cumulative variance threshold for auto n_components
    
    Returns:
        Dict with components, explained variance, transformed data
    """
    if features is None:
        feature_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    else:
        feature_cols = features
    
    data = df[feature_cols].dropna()
    X = data.values
    
    scaler_map = {
        "standard": StandardScaler(),
        "minmax": MinMaxScaler(),
        "robust": RobustScaler(),
        None: None,
    }
    
    if scaler and scaler in scaler_map:
        scaler_obj = scaler_map[scaler]
        X_scaled = scaler_obj.fit_transform(X)
    else:
        X_scaled = X
        scaler_obj = None
    
    if n_components is None:
        # Fit full PCA to determine n_components
        pca_full = PCA()
        pca_full.fit(X_scaled)
        cumsum = np.cumsum(pca_full.explained_variance_ratio_)
        n_components = int(np.argmax(cumsum >= variance_threshold)) + 1
        n_components = min(n_components, len(feature_cols))
    
    pca = PCA(n_components=n_components)
    X_pca = pca.fit_transform(X_scaled)
    
    # Component loadings
    loadings = pd.DataFrame(
        pca.components_.T,
        columns=[f"PC{i+1}" for i in range(n_components)],
        index=feature_cols,
    )
    
    # Transformed data
    pca_df = pd.DataFrame(
        X_pca,
        columns=[f"PC{i+1}" for i in range(n_components)],
        index=data.index,
    )
    
    result_df = df.loc[data.index].copy()
    for col in pca_df.columns:
        result_df[col] = pca_df[col]
    
    return {
        "model": "PCA",
        "n_components": n_components,
        "explained_variance_ratio": pca.explained_variance_ratio_.tolist(),
        "cumulative_variance": np.cumsum(pca.explained_variance_ratio_).tolist(),
        "loadings": loadings.to_dict(),
        "feature_names": feature_cols,
        "scaler": scaler,
        "results": result_df,
    }


def ica_analysis(
    df: pd.DataFrame,
    features: Optional[List[str]] = None,
    n_components: Optional[int] = None,
    scaler: str = "standard",
    random_state: int = 42,
) -> Dict[str, Any]:
    """Independent Component Analysis."""
    if features is None:
        feature_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    else:
        feature_cols = features
    
    data = df[feature_cols].dropna()
    X = data.values
    
    scaler_map = {
        "standard": StandardScaler(),
        "minmax": MinMaxScaler(),
        "robust": RobustScaler(),
        None: None,
    }
    
    if scaler and scaler in scaler_map:
        X_scaled = scaler_map[scaler].fit_transform(X)
    else:
        X_scaled = X
    
    if n_components is None:
        n_components = min(len(feature_cols), X.shape[0])
    
    ica = FastICA(n_components=n_components, random_state=random_state, max_iter=1000)
    X_ica = ica.fit_transform(X_scaled)
    
    ica_df = pd.DataFrame(
        X_ica,
        columns=[f"IC{i+1}" for i in range(n_components)],
        index=data.index,
    )
    
    result_df = df.loc[data.index].copy()
    for col in ica_df.columns:
        result_df[col] = ica_df[col]
    
    return {
        "model": "FastICA",
        "n_components": n_components,
        "mixing_matrix": ica.mixing_.tolist(),
        "feature_names": feature_cols,
        "results": result_df,
    }