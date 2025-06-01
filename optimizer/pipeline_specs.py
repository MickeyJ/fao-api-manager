class PipelineSpecsEnhancer:
    def __init__(self, optimization_results: Dict):
        self.optimization_results = optimization_results
        self.recommendations = optimization_results.get("recommendations", [])
        
    def enhance_csv_analysis(self, csv_analysis: Dict, table_name: str) -> Dict:
        """Enhance existing CSV analysis with optimization recommendations"""
        
    def should_use_area_fk(self) -> bool:
        # Check if areas should be normalized
        
    def should_use_item_fk(self) -> bool:
        # Check if items should be normalized
        
    def get_columns_to_exclude(self) -> Set[str]:
        # Get columns that should be excluded
        
    def get_foreign_key_mappings(self) -> Dict[str, str]:
        # Return mapping of columns to foreign key references