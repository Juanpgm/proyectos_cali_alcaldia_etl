# -*- coding: utf-8 -*-
"""
Google Maps Reverse Geocoding Module with Workload Identity Federation (WIF)

This module provides reverse geocoding functionality using Google Maps API
with Application Default Credentials (ADC) for authentication.

Features:
- Reverse geocoding (coordinates to address)
- Extraction of neighborhood (barrio/vereda) and district (comuna/corregimiento)
- Rate limiting and error handling
- Authentication with ADC/WIF

Author: AI Assistant
Version: 1.0
"""

import os
import sys
import json
import time
import pandas as pd
import googlemaps
from typing import Optional, Dict, Tuple, List
from google.auth import default
from google.auth.transport.requests import Request

# Add database to path for config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


class GoogleMapsGeocoder:
    """
    Google Maps Reverse Geocoder with ADC authentication.
    
    This class handles reverse geocoding using Google Maps API with
    Application Default Credentials for secure authentication.
    """
    
    def __init__(self, api_key: Optional[str] = None, use_adc: bool = True):
        """
        Initialize Google Maps client with ADC or API key.
        
        Args:
            api_key: Google Maps API key (optional if using ADC)
            use_adc: If True, use Application Default Credentials (recommended for WIF)
        """
        self.api_key = api_key
        self.use_adc = use_adc
        self.client = None
        self.credentials = None
        self.request_count = 0
        self.rate_limit_delay = 0.1  # 100ms delay between requests
        
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Google Maps client with appropriate authentication."""
        try:
            if self.use_adc:
                # Use Application Default Credentials (WIF)
                print("🔐 Authenticating with Application Default Credentials (ADC)...")
                
                # Get ADC credentials
                credentials, project = default(
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                
                # Refresh credentials if needed
                if not credentials.valid:
                    if credentials.expired and credentials.refresh_token:
                        credentials.refresh(Request())
                
                self.credentials = credentials
                
                # For Google Maps API, we still need an API key
                # Check if API key is in environment
                api_key_from_env = os.getenv('GOOGLE_MAPS_API_KEY')
                
                if api_key_from_env:
                    self.api_key = api_key_from_env
                    self.client = googlemaps.Client(key=self.api_key)
                    print(f"✅ Google Maps client initialized with ADC + API Key")
                    print(f"   Project: {project}")
                else:
                    print("⚠️  Warning: GOOGLE_MAPS_API_KEY not found in environment")
                    print("   Set it in .env.prod or .env.local:")
                    print("   GOOGLE_MAPS_API_KEY=your-api-key-here")
                    raise ValueError("Google Maps API Key required")
                    
            else:
                # Use direct API key
                if not self.api_key:
                    self.api_key = os.getenv('GOOGLE_MAPS_API_KEY')
                
                if not self.api_key:
                    raise ValueError("Google Maps API Key required")
                
                self.client = googlemaps.Client(key=self.api_key)
                print("✅ Google Maps client initialized with API Key")
        
        except Exception as e:
            print(f"❌ Error initializing Google Maps client: {e}")
            raise
    
    def reverse_geocode(
        self, 
        latitude: float, 
        longitude: float,
        language: str = 'es'
    ) -> Optional[List[Dict]]:
        """
        Perform reverse geocoding for given coordinates.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            language: Language for results (default: 'es' for Spanish)
            
        Returns:
            List of geocoding results or None if error
        """
        if not self.client:
            print("❌ Google Maps client not initialized")
            return None
        
        try:
            # Rate limiting
            if self.request_count > 0:
                time.sleep(self.rate_limit_delay)
            
            # Perform reverse geocoding
            results = self.client.reverse_geocode(
                (latitude, longitude),
                language=language
            )
            
            self.request_count += 1
            
            return results if results else None
            
        except googlemaps.exceptions.ApiError as e:
            print(f"❌ Google Maps API Error: {e}")
            return None
        except Exception as e:
            print(f"❌ Error in reverse geocoding: {e}")
            return None
    
    def extract_barrio_vereda(self, geocode_results: List[Dict]) -> Optional[str]:
        """
        Extract neighborhood (barrio/vereda) from geocoding results.
        
        Looks for address components with type 'sublocality' as primary source.
        
        Args:
            geocode_results: Results from reverse_geocode
            
        Returns:
            Neighborhood name or None if not found
        """
        if not geocode_results:
            return None
        
        # Address component types to look for (in order of preference)
        target_types = [
            'sublocality',            # Sub-localidad (PRIMARY)
            'sublocality_level_1',    # Sub-localidad nivel 1
            'locality'                # Localidad (fallback)
        ]
        
        for result in geocode_results:
            address_components = result.get('address_components', [])
            
            for target_type in target_types:
                for component in address_components:
                    types = component.get('types', [])
                    if target_type in types:
                        return component.get('long_name')
        
        return None
    
    def extract_comuna_corregimiento(self, geocode_results: List[Dict]) -> Optional[str]:
        """
        Extract district (comuna/corregimiento) from geocoding results.
        
        Looks for address components with type 'neighborhood' as primary source.
        If 'neighborhood' returns 'Cali', explores other components for Comuna/Corregimiento.
        
        Args:
            geocode_results: Results from reverse_geocode
            
        Returns:
            District name or None if not found
        """
        if not geocode_results:
            return None
        
        # First pass: Look for 'neighborhood' type
        first_found = None
        all_components = []
        
        for result in geocode_results:
            address_components = result.get('address_components', [])
            
            # Collect all components for analysis
            for component in address_components:
                long_name = component.get('long_name', '')
                types = component.get('types', [])
                all_components.append({
                    'name': long_name,
                    'types': types
                })
                
                # Check if this is a neighborhood
                if 'neighborhood' in types and not first_found:
                    first_found = long_name
        
        # If we found a neighborhood and it's NOT "Cali", return it
        if first_found and first_found != 'Cali':
            return first_found
        
        # If we got "Cali" or nothing, search for components with "Comuna" or "Corregimiento"
        # in the name across all types
        for comp in all_components:
            name = comp['name']
            if name and isinstance(name, str):
                name_upper = name.upper()
                # Check if name contains COMUNA or CORREGIMIENTO
                if 'COMUNA' in name_upper or 'CORREGIMIENTO' in name_upper:
                    return name
        
        # Fallback: Try administrative_area_level_3 or sublocality
        target_types = ['administrative_area_level_3', 'sublocality_level_1', 'sublocality']
        
        for comp in all_components:
            for target_type in target_types:
                if target_type in comp['types']:
                    name = comp['name']
                    if name and name != 'Cali':
                        return name
        
        # Last resort: return what we found (even if it's "Cali")
        return first_found if first_found else None
    
    def get_address_info(
        self,
        latitude: float,
        longitude: float
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Get both barrio/vereda and comuna/corregimiento from coordinates.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Tuple of (barrio_vereda, comuna_corregimiento)
        """
        results = self.reverse_geocode(latitude, longitude)
        
        if not results:
            return None, None
        
        barrio = self.extract_barrio_vereda(results)
        comuna = self.extract_comuna_corregimiento(results)
        
        return barrio, comuna
    
    def forward_geocode(
        self,
        address: str,
        language: str = 'es',
        region: str = 'co'
    ) -> Optional[Dict]:
        """
        Perform forward geocoding (address to coordinates).
        
        Args:
            address: Full address string to geocode
            language: Language for results (default: 'es' for Spanish)
            region: Region bias (default: 'co' for Colombia)
            
        Returns:
            Geocoding result with coordinates or None if error
        """
        if not self.client:
            print("❌ Google Maps client not initialized")
            return None
        
        try:
            # Rate limiting
            if self.request_count > 0:
                time.sleep(self.rate_limit_delay)
            
            # Perform forward geocoding
            results = self.client.geocode(
                address,
                language=language,
                region=region
            )
            
            self.request_count += 1
            
            if results and len(results) > 0:
                return results[0]
            
            return None
            
        except googlemaps.exceptions.ApiError as e:
            print(f"❌ Google Maps API Error: {e}")
            return None
        except Exception as e:
            print(f"❌ Error in forward geocoding: {e}")
            return None
    
    def get_coordinates_from_address(
        self,
        address: str
    ) -> Optional[Tuple[float, float]]:
        """
        Get coordinates (lat, lon) from address string.

        Args:
            address: Full address string

        Returns:
            Tuple of (latitude, longitude) or None if not found
        """
        result = self.forward_geocode(address)

        if not result:
            return None

        geometry = result.get('geometry', {})
        location = geometry.get('location', {})

        lat = location.get('lat')
        lon = location.get('lng')

        if lat is not None and lon is not None:
            return lat, lon

        return None

    def geocode_address(self, address: str) -> Optional[Dict]:
        """
        Geocode an address using Google Maps API.

        Returns dict with 'lat' and 'lon' keys, or None if geocoding fails.

        This is a convenience function that wraps get_coordinates_from_address.
        """
        coords = self.get_coordinates_from_address(address)
        if coords:
            lat, lon = coords
            return {'lat': lat, 'lon': lon}
        return None
    
    def process_dataframe(
        self,
        df: pd.DataFrame,
        geometry_column: str = 'geometry',
        output_barrio_column: str = 'barrio_vereda_val_s3',
        output_comuna_column: str = 'comuna_corregimiento_val_s3',
        filter_column: Optional[str] = 'corregir',
        filter_value: str = 'INTENTAR GEORREFERENCIAR',
        max_requests: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Process a DataFrame with reverse geocoding for filtered records.
        
        Args:
            df: Input DataFrame
            geometry_column: Column containing geometry (GeoJSON format)
            output_barrio_column: Column name for barrio/vereda results
            output_comuna_column: Column name for comuna/corregimiento results
            filter_column: Column to filter records (optional)
            filter_value: Value to match for filtering (default: 'INTENTAR GEORREFERENCIAR')
            max_requests: Maximum number of requests to make (optional, for testing)
            
        Returns:
            DataFrame with new columns
        """
        result_df = df.copy()
        
        # Initialize output columns
        result_df[output_barrio_column] = "ERROR"
        result_df[output_comuna_column] = "ERROR"
        
        print(f"\n{'='*60}")
        print("GOOGLE MAPS REVERSE GEOCODING")
        print(f"{'='*60}")
        
        # Filter records to process
        if filter_column and filter_column in result_df.columns:
            mask = result_df[filter_column] == filter_value
            records_to_process = result_df[mask].copy()
            print(f"📍 Records filtered by {filter_column}='{filter_value}': {len(records_to_process)}")
        else:
            records_to_process = result_df.copy()
            print(f"📍 Total records: {len(records_to_process)}")
        
        # EXCLUDE records with invalid geometry BEFORE processing
        print(f"\n🔍 Filtering valid geometries...")
        invalid_geometry_mask = (
            records_to_process[geometry_column].isna() |
            (records_to_process[geometry_column] == '') |
            (records_to_process[geometry_column] == 'ERROR') |
            (records_to_process[geometry_column] == 'REVISAR') |
            (records_to_process[geometry_column] == 'null') |
            (records_to_process[geometry_column] == 'NULL')
        )
        
        excluded_count = invalid_geometry_mask.sum()
        records_to_process = records_to_process[~invalid_geometry_mask].copy()
        
        print(f"   ❌ Excluded (invalid geometry): {excluded_count}")
        print(f"   ✅ Valid geometries to process: {len(records_to_process)}")
        
        # Apply max_requests limit if specified
        if max_requests and len(records_to_process) > max_requests:
            print(f"⚠️  Limiting to first {max_requests} requests for testing")
            records_to_process = records_to_process.head(max_requests)
        
        # Track statistics
        success_count = 0
        error_count = 0
        barrio_found = 0
        comuna_found = 0
        both_found = 0
        
        print(f"\n🔄 Starting reverse geocoding...")
        print(f"   Rate limit delay: {self.rate_limit_delay}s between requests")
        
        # Process each record (all should have valid geometry at this point)
        for idx, row in records_to_process.iterrows():
            try:
                # Extract coordinates from geometry
                geometry = row.get(geometry_column)
                
                lat = None
                lon = None
                
                # Handle Shapely Point objects (from GeoDataFrame)
                if hasattr(geometry, 'geom_type'):
                    if geometry.geom_type == 'Point':
                        # Custom format: Point(lat, lon) - NOT standard Shapely!
                        lat = geometry.x  # In this project: x = latitude
                        lon = geometry.y  # In this project: y = longitude
                
                # Handle JSON string geometries
                elif isinstance(geometry, str):
                    try:
                        geom_obj = json.loads(geometry)
                        # Extract coordinates (custom format: [lat, lon])
                        if isinstance(geom_obj, dict) and 'coordinates' in geom_obj:
                            coords = geom_obj['coordinates']
                            if isinstance(coords, list) and len(coords) >= 2:
                                lat, lon = coords[0], coords[1]  # Custom format: [latitude, longitude]
                    except json.JSONDecodeError:
                        error_count += 1
                        continue
                
                # Handle dict geometries
                elif isinstance(geometry, dict) and 'coordinates' in geometry:
                    coords = geometry['coordinates']
                    if isinstance(coords, list) and len(coords) >= 2:
                        lat, lon = coords[0], coords[1]  # Custom format: [latitude, longitude]
                
                # If we successfully extracted coordinates, perform reverse geocoding
                if lat is not None and lon is not None:
                    # Perform reverse geocoding (API expects lat, lon)
                    barrio, comuna = self.get_address_info(lat, lon)
                    
                    # Update results
                    if barrio:
                        result_df.at[idx, output_barrio_column] = barrio
                        barrio_found += 1
                    
                    if comuna:
                        result_df.at[idx, output_comuna_column] = comuna
                        comuna_found += 1
                    
                    if barrio and comuna:
                        both_found += 1
                        success_count += 1
                    elif barrio or comuna:
                        success_count += 1
                    else:
                        error_count += 1
                    
                    # Progress indicator
                    if (success_count + error_count) % 10 == 0:
                        print(f"   Processed: {success_count + error_count}/{len(records_to_process)} records...")
                else:
                    error_count += 1
                    
            except Exception as e:
                print(f"   ⚠️  Error processing record {idx}: {e}")
                error_count += 1
                continue
        
        # Print summary
        total_processed = success_count + error_count
        print(f"\n✅ Reverse geocoding completed!")
        print(f"\n📊 Statistics:")
        print(f"   Total processed: {total_processed}")
        print(f"   Successful: {success_count} ({success_count/total_processed*100:.1f}%)")
        print(f"   Errors: {error_count} ({error_count/total_processed*100:.1f}%)")
        print(f"   Barrio/Vereda found: {barrio_found}")
        print(f"   Comuna/Corregimiento found: {comuna_found}")
        print(f"   Both found: {both_found}")
        print(f"   API requests made: {self.request_count}")
        
        return result_df
    
    def process_forward_geocoding(
        self,
        df: pd.DataFrame,
        address_column: str = 'direccion_api',
        output_geometry_column: str = 'geometry_val_s2',
        filter_column: Optional[str] = 'corregir',
        filter_value: str = 'INTENTAR GEORREFERENCIAR'
    ) -> pd.DataFrame:
        """
        Process DataFrame with forward geocoding (address to coordinates).
        
        Args:
            df: Input DataFrame
            address_column: Column containing full address strings
            output_geometry_column: Column name for geometry results (GeoJSON Point format)
            filter_column: Column to filter records (optional)
            filter_value: Value to match for filtering (default: 'INTENTAR GEORREFERENCIAR')
            
        Returns:
            DataFrame with new geometry column
        """
        result_df = df.copy()
        
        # Initialize output column
        result_df[output_geometry_column] = "ERROR"
        
        print(f"\n{'='*60}")
        print("GOOGLE MAPS FORWARD GEOCODING (Address → Coordinates)")
        print(f"{'='*60}")
        
        # Filter records to process
        if filter_column and filter_column in result_df.columns:
            mask = (
                (result_df[filter_column] == filter_value) &
                (result_df[address_column].notna()) &
                (result_df[address_column] != '') &
                (result_df[address_column] != 'ERROR')
            )
            valid_addresses = result_df[mask].copy()
            print(f"📍 Records filtered by {filter_column}='{filter_value}': {len(valid_addresses)}")
        else:
            # Get records with valid addresses
            valid_addresses = result_df[
                (result_df[address_column].notna()) &
                (result_df[address_column] != '') &
                (result_df[address_column] != 'ERROR')
            ].copy()
            print(f"📍 Records with valid addresses: {len(valid_addresses)}")
        
        # Track statistics
        success_count = 0
        error_count = 0
        
        print(f"\n🔄 Starting forward geocoding...")
        print(f"   Rate limit delay: {self.rate_limit_delay}s between requests")
        
        # Process each record
        for idx, row in valid_addresses.iterrows():
            try:
                address = row.get(address_column)
                
                # Get coordinates
                coords = self.get_coordinates_from_address(address)
                
                if coords:
                    lat, lon = coords
                    
                    # Create Point geometry (custom format: [lat, lon] - NOT standard GeoJSON!)
                    # This matches the format used in create_point_from_coordinates()
                    geometry_geojson = {
                        "type": "Point",
                        "coordinates": [lat, lon]  # Custom format: [latitude, longitude]
                    }
                    
                    # Store as JSON string (matching existing format)
                    result_df.at[idx, output_geometry_column] = json.dumps(geometry_geojson)
                    success_count += 1
                else:
                    error_count += 1
                
                # Progress indicator
                if (success_count + error_count) % 10 == 0:
                    print(f"   Processed: {success_count + error_count}/{len(valid_addresses)} records...")
                    
            except Exception as e:
                print(f"   ⚠️  Error processing record {idx}: {e}")
                error_count += 1
                continue
        
        # Print summary
        total_processed = success_count + error_count
        print(f"\n✅ Forward geocoding completed!")
        print(f"\n📊 Statistics:")
        print(f"   Total processed: {total_processed}")
        print(f"   Successful: {success_count} ({success_count/total_processed*100:.1f}%)")
        print(f"   Errors: {error_count} ({error_count/total_processed*100:.1f}%)")
        print(f"   API requests made: {self.request_count}")
        
        return result_df


def reverse_geocode_gdf_geolocalizar(
    input_file: str,
    output_file: Optional[str] = None,
    max_requests: Optional[int] = None
) -> pd.DataFrame:
    """
    Main function to perform reverse geocoding on gdf_geolocalizar file.
    
    Args:
        input_file: Path to input Excel/CSV file
        output_file: Path to output file (optional, will overwrite input if not specified)
        max_requests: Maximum number of requests for testing (optional)
        
    Returns:
        Processed DataFrame
    """
    print("="*80)
    print("REVERSE GEOCODING - GDF_GEOLOCALIZAR")
    print("="*80)
    
    # Load data
    print(f"\n📂 Loading data from: {input_file}")
    
    if input_file.endswith('.xlsx'):
        df = pd.read_excel(input_file)
    elif input_file.endswith('.csv'):
        df = pd.read_csv(input_file)
    else:
        raise ValueError("Input file must be .xlsx or .csv")
    
    print(f"✓ Loaded {len(df)} records")
    print(f"   Columns: {list(df.columns)}")
    
    # Initialize geocoder
    geocoder = GoogleMapsGeocoder(use_adc=True)
    
    # Process data
    result_df = geocoder.process_dataframe(
        df,
        geometry_column='geometry',
        output_barrio_column='barrio_vereda_val_s3',
        output_comuna_column='comuna_corregimiento_val_s3',
        filter_column='corregir',
        filter_value='INTENTAR GEORREFERENCIAR',
        max_requests=max_requests
    )
    
    # Save results
    if output_file is None:
        output_file = input_file
    
    print(f"\n💾 Saving results to: {output_file}")
    
    if output_file.endswith('.xlsx'):
        result_df.to_excel(output_file, index=False, engine='xlsxwriter')
    elif output_file.endswith('.csv'):
        result_df.to_csv(output_file, index=False)
    
    file_size = os.path.getsize(output_file) / 1024
    print(f"✓ File saved successfully!")
    print(f"   File size: {file_size:.1f} KB")
    
    # Print final distribution
    print(f"\n📊 Final distribution:")
    
    if 'barrio_vereda_val_s3' in result_df.columns:
        barrio_counts = result_df['barrio_vereda_val_s3'].value_counts()
        error_count = (result_df['barrio_vereda_val_s3'] == 'ERROR').sum()
        success_count = len(result_df) - error_count
        print(f"\n   Barrio/Vereda (barrio_vereda_val_s3):")
        print(f"   - Found: {success_count}")
        print(f"   - ERROR: {error_count}")
    
    if 'comuna_corregimiento_val_s3' in result_df.columns:
        comuna_counts = result_df['comuna_corregimiento_val_s3'].value_counts()
        error_count = (result_df['comuna_corregimiento_val_s3'] == 'ERROR').sum()
        success_count = len(result_df) - error_count
        print(f"\n   Comuna/Corregimiento (comuna_corregimiento_val_s3):")
        print(f"   - Found: {success_count}")
        print(f"   - ERROR: {error_count}")
    
    return result_df


if __name__ == "__main__":
    """
    Example usage:
    
    python google_maps_geocoder.py
    """
    
    # Path to gdf_geolocalizar file
    input_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'transformation_app',
        'app_outputs',
        'unidades_proyecto_outputs',
        'gdf_geolocalizar.xlsx'
    )
    
    # Test with limited requests first
    print("🧪 Running test with first 5 records...")
    result_df = reverse_geocode_gdf_geolocalizar(
        input_path,
        max_requests=5
    )
    
    print("\n" + "="*80)
    print("✅ TEST COMPLETED")
    print("="*80)
    print("\nTo process all records, run:")
    print(f"  python {__file__}")
    print("\nOr import and use:")
    print("  from utils.google_maps_geocoder import reverse_geocode_gdf_geolocalizar")
    print("  result_df = reverse_geocode_gdf_geolocalizar(input_path, max_requests=None)")
