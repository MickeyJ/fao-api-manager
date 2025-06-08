import json
import zipfile
from pathlib import Path
from fao.src.db.database import run_with_session
from .area_codes.__main__ import run_all as run_area_codes
from .item_codes.__main__ import run_all as run_item_codes
from .elements.__main__ import run_all as run_elements
from .population_age_groups.__main__ import run_all as run_population_age_groups
from .sexs.__main__ import run_all as run_sexs
from .flags.__main__ import run_all as run_flags
from .currencies.__main__ import run_all as run_currencies
from .sources.__main__ import run_all as run_sources
from .surveys.__main__ import run_all as run_surveys
from .releases.__main__ import run_all as run_releases
from .indicators.__main__ import run_all as run_indicators
from .purposes.__main__ import run_all as run_purposes
from .donors.__main__ import run_all as run_donors
from .food_groups.__main__ import run_all as run_food_groups
from .geographic_levels.__main__ import run_all as run_geographic_levels
from .aquastat.__main__ import run_all as run_aquastat
from .asti_expenditures.__main__ import run_all as run_asti_expenditures
from .asti_researchers.__main__ import run_all as run_asti_researchers
from .climate_change_emissions_indicators.__main__ import run_all as run_climate_change_emissions_indicators
from .commodity_balances_non_food_2013_old_methodology.__main__ import run_all as run_commodity_balances_non_food_2013_old_methodology
from .commodity_balances_non_food_2010.__main__ import run_all as run_commodity_balances_non_food_2010
from .commodity_balances_non_food.__main__ import run_all as run_commodity_balances_non_food
from .consumer_price_indices.__main__ import run_all as run_consumer_price_indices
from .cost_affordability_healthy_diet_co_ahd.__main__ import run_all as run_cost_affordability_healthy_diet_co_ahd
from .deflators.__main__ import run_all as run_deflators
from .development_assistance_to_agriculture.__main__ import run_all as run_development_assistance_to_agriculture
from .emissions_agriculture_energy.__main__ import run_all as run_emissions_agriculture_energy
from .emissions_crops.__main__ import run_all as run_emissions_crops
from .emissions_drained_organic_soils.__main__ import run_all as run_emissions_drained_organic_soils
from .emissions_land_use_fires.__main__ import run_all as run_emissions_land_use_fires
from .emissions_land_use_forests.__main__ import run_all as run_emissions_land_use_forests
from .emissions_livestock.__main__ import run_all as run_emissions_livestock
from .emissions_pre_post_production.__main__ import run_all as run_emissions_pre_post_production
from .emissions_totals.__main__ import run_all as run_emissions_totals
from .employment_indicators_agriculture.__main__ import run_all as run_employment_indicators_agriculture
from .employment_indicators_rural.__main__ import run_all as run_employment_indicators_rural
from .environment_bioenergy.__main__ import run_all as run_environment_bioenergy
from .environment_cropland_nutrient_budget.__main__ import run_all as run_environment_cropland_nutrient_budget
from .environment_emissions_by_sector.__main__ import run_all as run_environment_emissions_by_sector
from .environment_emissions_intensities.__main__ import run_all as run_environment_emissions_intensities
from .environment_food_waste_disposal.__main__ import run_all as run_environment_food_waste_disposal
from .environment_land_cover.__main__ import run_all as run_environment_land_cover
from .environment_land_use.__main__ import run_all as run_environment_land_use
from .environment_livestock_manure.__main__ import run_all as run_environment_livestock_manure
from .environment_livestock_patterns.__main__ import run_all as run_environment_livestock_patterns
from .environment_pesticides.__main__ import run_all as run_environment_pesticides
from .environment_soil_nutrient_budget.__main__ import run_all as run_environment_soil_nutrient_budget
from .environment_temperature_change.__main__ import run_all as run_environment_temperature_change
from .exchange_rate.__main__ import run_all as run_exchange_rate
from .fertilizers_detailed_trade_matrix.__main__ import run_all as run_fertilizers_detailed_trade_matrix
from .food_balance_sheets_historic.__main__ import run_all as run_food_balance_sheets_historic
from .food_balance_sheets.__main__ import run_all as run_food_balance_sheets
from .food_aid_shipments_wfp.__main__ import run_all as run_food_aid_shipments_wfp
from .food_and_diet_individual_quantitative_dietary_data.__main__ import run_all as run_food_and_diet_individual_quantitative_dietary_data
from .food_security_data.__main__ import run_all as run_food_security_data
from .forestry.__main__ import run_all as run_forestry
from .forestry_pulp_paper_survey.__main__ import run_all as run_forestry_pulp_paper_survey
from .forestry_trade_flows.__main__ import run_all as run_forestry_trade_flows
from .household_consumption_and_expenditure_surveys_food_and_diet.__main__ import run_all as run_household_consumption_and_expenditure_surveys_food_and_diet
from .indicators_from_household_surveys.__main__ import run_all as run_indicators_from_household_surveys
from .individual_quantitative_dietary_data_food_and_diet.__main__ import run_all as run_individual_quantitative_dietary_data_food_and_diet
from .inputs_fertilizers_archive.__main__ import run_all as run_inputs_fertilizers_archive
from .inputs_fertilizers_nutrient.__main__ import run_all as run_inputs_fertilizers_nutrient
from .inputs_fertilizers_product.__main__ import run_all as run_inputs_fertilizers_product
from .inputs_land_use.__main__ import run_all as run_inputs_land_use
from .inputs_pesticides_trade.__main__ import run_all as run_inputs_pesticides_trade
from .inputs_pesticides_use.__main__ import run_all as run_inputs_pesticides_use
from .investment_capital_stock.__main__ import run_all as run_investment_capital_stock
from .investment_country_investment_statistics_profile.__main__ import run_all as run_investment_country_investment_statistics_profile
from .investment_credit_agriculture.__main__ import run_all as run_investment_credit_agriculture
from .investment_foreign_direct_investment.__main__ import run_all as run_investment_foreign_direct_investment
from .investment_government_expenditure.__main__ import run_all as run_investment_government_expenditure
from .investment_machinery_archive.__main__ import run_all as run_investment_machinery_archive
from .investment_machinery.__main__ import run_all as run_investment_machinery
from .macro_statistics_key_indicators.__main__ import run_all as run_macro_statistics_key_indicators
from .minimum_dietary_diversity_for_women_mdd_w_food_and_diet.__main__ import run_all as run_minimum_dietary_diversity_for_women_mdd_w_food_and_diet
from .population.__main__ import run_all as run_population
from .prices_archive.__main__ import run_all as run_prices_archive
from .prices.__main__ import run_all as run_prices
from .production_crops_livestock.__main__ import run_all as run_production_crops_livestock
from .production_indices.__main__ import run_all as run_production_indices
from .sdg_bulk_downloads.__main__ import run_all as run_sdg_bulk_downloads
from .sua_crops_livestock.__main__ import run_all as run_sua_crops_livestock
from .supply_utilization_accounts_food_and_diet.__main__ import run_all as run_supply_utilization_accounts_food_and_diet

def ensure_zips_extracted():
    """Extract ZIP files if needed based on manifest"""
    manifest_path = Path(__file__).parent.parent.parent / "extraction_manifest.json"
    
    if not manifest_path.exists():
        print("⚠️  No extraction manifest found - ZIPs may not be extracted")
        return
    
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    
    print("📦 Checking ZIP extractions...")
    
    for extraction in manifest["extractions"]:
        zip_path = Path(extraction["zip_path"])
        extract_dir = Path(extraction["extract_dir"])
        
        # Check if extraction is needed
        if not extract_dir.exists() or not _extraction_is_current(zip_path, extract_dir):
            print(f"📂 Extracting {zip_path.name}...")
            extract_dir.mkdir(exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(extract_dir)
        else:
            print(f"✅ {zip_path.name} already extracted")

def _extraction_is_current(zip_path: Path, extract_dir: Path) -> bool:
    """Check if extraction is up to date"""
    if not extract_dir.exists():
        return False
    
    # Simple check: if extract dir is newer than ZIP file
    try:
        zip_mtime = zip_path.stat().st_mtime
        extract_mtime = extract_dir.stat().st_mtime
        return extract_mtime >= zip_mtime
    except (OSError, FileNotFoundError):
        return False

def run_all_pipelines(db):
    ensure_zips_extracted()
    print("🚀 Starting all data pipelines...")
    run_area_codes(db)
    run_item_codes(db)
    run_elements(db)
    run_population_age_groups(db)
    run_sexs(db)
    run_flags(db)
    run_currencies(db)
    run_sources(db)
    run_surveys(db)
    run_releases(db)
    run_indicators(db)
    run_purposes(db)
    run_donors(db)
    run_food_groups(db)
    run_geographic_levels(db)
    run_aquastat(db)
    run_asti_expenditures(db)
    run_asti_researchers(db)
    run_climate_change_emissions_indicators(db)
    run_commodity_balances_non_food_2013_old_methodology(db)
    run_commodity_balances_non_food_2010(db)
    run_commodity_balances_non_food(db)
    run_consumer_price_indices(db)
    run_cost_affordability_healthy_diet_co_ahd(db)
    run_deflators(db)
    run_development_assistance_to_agriculture(db)
    run_emissions_agriculture_energy(db)
    run_emissions_crops(db)
    run_emissions_drained_organic_soils(db)
    run_emissions_land_use_fires(db)
    run_emissions_land_use_forests(db)
    run_emissions_livestock(db)
    run_emissions_pre_post_production(db)
    run_emissions_totals(db)
    run_employment_indicators_agriculture(db)
    run_employment_indicators_rural(db)
    run_environment_bioenergy(db)
    run_environment_cropland_nutrient_budget(db)
    run_environment_emissions_by_sector(db)
    run_environment_emissions_intensities(db)
    run_environment_food_waste_disposal(db)
    run_environment_land_cover(db)
    run_environment_land_use(db)
    run_environment_livestock_manure(db)
    run_environment_livestock_patterns(db)
    run_environment_pesticides(db)
    run_environment_soil_nutrient_budget(db)
    run_environment_temperature_change(db)
    run_exchange_rate(db)
    run_fertilizers_detailed_trade_matrix(db)
    run_food_balance_sheets_historic(db)
    run_food_balance_sheets(db)
    run_food_aid_shipments_wfp(db)
    run_food_and_diet_individual_quantitative_dietary_data(db)
    run_food_security_data(db)
    run_forestry(db)
    run_forestry_pulp_paper_survey(db)
    run_forestry_trade_flows(db)
    run_household_consumption_and_expenditure_surveys_food_and_diet(db)
    run_indicators_from_household_surveys(db)
    run_individual_quantitative_dietary_data_food_and_diet(db)
    run_inputs_fertilizers_archive(db)
    run_inputs_fertilizers_nutrient(db)
    run_inputs_fertilizers_product(db)
    run_inputs_land_use(db)
    run_inputs_pesticides_trade(db)
    run_inputs_pesticides_use(db)
    run_investment_capital_stock(db)
    run_investment_country_investment_statistics_profile(db)
    run_investment_credit_agriculture(db)
    run_investment_foreign_direct_investment(db)
    run_investment_government_expenditure(db)
    run_investment_machinery_archive(db)
    run_investment_machinery(db)
    run_macro_statistics_key_indicators(db)
    run_minimum_dietary_diversity_for_women_mdd_w_food_and_diet(db)
    run_population(db)
    run_prices_archive(db)
    run_prices(db)
    run_production_crops_livestock(db)
    run_production_indices(db)
    run_sdg_bulk_downloads(db)
    run_sua_crops_livestock(db)
    run_supply_utilization_accounts_food_and_diet(db)
    print("✅ All pipelines complete!")

if __name__ == "__main__":
    run_with_session(run_all_pipelines)