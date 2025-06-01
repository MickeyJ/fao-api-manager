@dataclass
class OptimizationRecommendation:
    table_name: str
    action: str
    details: Dict
    estimated_savings: str

class RecommendationGenerator:
    def __init__(self, analysis_results: Dict):
        self.analysis_results = analysis_results
        
    def generate_recommendations(self) -> List[OptimizationRecommendation]:
        # Logic to generate recommendations from analysis results
        
    def prioritize_recommendations(self, recommendations) -> List[str]:
        # Prioritization logic