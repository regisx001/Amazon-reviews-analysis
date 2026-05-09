import { env } from '$env/dynamic/public';

export type StatsResponse = {
    total: number;
    positive: number;
    neutral: number;
    negative: number;
};

export type PredictionsByDateResponse = {
    labels: string[];
    positive: number[];
    neutral: number[];
    negative: number[];
};

export type ProductScoringResponse = {
    product_id: string;
    total: number;
    counts: {
        positive: number;
        neutral: number;
        negative: number;
    };
    percentages: {
        positive: number;
        neutral: number;
        negative: number;
    };
};

export type RecentPrediction = {
    Id?: string;
    ProductId?: string;
    ProfileName?: string;
    Score?: string | number;
    PredictedSentiment?: string;
    Summary?: string;
    ProcessingTime?: string | number;
};

export type RecentPredictionsResponse = {
    predictions: RecentPrediction[];
};

export type TopProductsResponse = {
    labels: string[];
    counts: number[];
};

export type ProductSentimentItem = {
    product_id: string;
    total: number;
    counts: {
        positive: number;
        neutral: number;
        negative: number;
    };
    percentages: {
        positive: number;
        neutral: number;
        negative: number;
    };
};

export type ProductSentimentListResponse = {
    products: ProductSentimentItem[];
};

export type DriftStatusResponse = {
    evaluated_at: string | null;
    total: number;
    neutral_ratio: number;
    negative_ratio: number;
    drift_detected: boolean;
    alerts: string[];
    counts: {
        positive: number;
        neutral: number;
        negative: number;
    };
    reason?: string | null;
};

export type AggregationStatusResponse = {
    last_aggregated_at: string | null;
    monthly_last_aggregated_at: string | null;
    product_last_aggregated_at: string | null;
};

export type ModelClassMetrics = {
    precision: number;
    recall: number;
    f1: number;
};

export type ModelMetrics = {
    accuracy?: number | null;
    f1_weighted?: number | null;
    precision_weighted?: number | null;
    recall_weighted?: number | null;
    per_class?: Record<string, ModelClassMetrics> | null;
};

export type ConfusionMatrix = {
    labels: string[];
    matrix: number[][];
};

export type ModelMetadata = {
    model_name?: string | null;
    model_version?: string | null;
    trained_at?: string | null;
    dataset?: string | null;
    notes?: string | null;
    parameters?: Record<string, unknown> | null;
};

export type ModelInsightsResponse = {
    evaluated_at: string | null;
    status: string;
    metrics?: ModelMetrics | null;
    confusion_matrix?: ConfusionMatrix | null;
    metadata?: ModelMetadata | null;
    source?: Record<string, string> | null;
    errors: string[];
};

const DEFAULT_BASE = "http://localhost:8000";

function normalizeBase(raw: string): string {
    const trimmed = (raw || "").trim();
    if (!trimmed) return DEFAULT_BASE;
    if (trimmed.startsWith("/")) return trimmed.replace(/\/+$/, "");
    if (!/^https?:\/\//i.test(trimmed)) {
        return `http://${trimmed}`.replace(/\/+$/, "");
    }
    return trimmed.replace(/\/+$/, "");
}

export const API_BASE = normalizeBase(env.PUBLIC_API_BASE_URL || DEFAULT_BASE);

async function apiFetch<T>(path: string): Promise<T> {
    const base = API_BASE.startsWith("/")
        ? API_BASE
        : new URL(path, API_BASE).toString();
    const url = API_BASE.startsWith("/") ? `${base}${path}` : base;
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
    }
    return response.json() as Promise<T>;
}

export function fetchStats(): Promise<StatsResponse> {
    return apiFetch<StatsResponse>("/api/stats");
}

export function fetchPredictionsByDate(): Promise<PredictionsByDateResponse> {
    return apiFetch<PredictionsByDateResponse>("/api/predictions-by-date");
}

export function fetchProductScoring(productId: string): Promise<ProductScoringResponse> {
    const id = encodeURIComponent(productId);
    return apiFetch<ProductScoringResponse>(`/api/product-scoring?product_id=${id}`);
}

export function fetchRecentPredictions(limit = 20): Promise<RecentPredictionsResponse> {
    return apiFetch<RecentPredictionsResponse>(`/api/recent-predictions?limit=${limit}`);
}

export function fetchTopProducts(limit = 8): Promise<TopProductsResponse> {
    return apiFetch<TopProductsResponse>(`/api/top-products?limit=${limit}`);
}

export function fetchTopProductsSentiment(limit = 8): Promise<ProductSentimentListResponse> {
    return apiFetch<ProductSentimentListResponse>(`/api/top-products-sentiment?limit=${limit}`);
}

export function fetchDriftStatus(): Promise<DriftStatusResponse> {
    return apiFetch<DriftStatusResponse>("/api/drift-status");
}

export function fetchAggregationStatus(): Promise<AggregationStatusResponse> {
    return apiFetch<AggregationStatusResponse>("/api/aggregation-status");
}

export function fetchModelInsights(): Promise<ModelInsightsResponse> {
    return apiFetch<ModelInsightsResponse>("/api/model-insights");
}

export type RecommendedProduct = {
    product_id: string;
    total_reviews: number;
    positive_count: number;
    negative_count: number;
    satisfaction_rate: number;
    dissatisfaction_rate: number;
};

export type RecommendationsResponse = {
    updated_at: string;
    week_label: string;
    top_recommended: RecommendedProduct[];
    quality_alerts: RecommendedProduct[];
    metadata: {
        confidence_threshold: number;
        total_eligible: number;
    };
    status?: string;
    message?: string;
};

export function fetchRecommendations(): Promise<RecommendationsResponse> {
    return apiFetch<RecommendationsResponse>("/api/recommendations");
}
