import requests
import json
import csv
import os
import time
from typing import List, Dict, Set
from bs4 import BeautifulSoup
import threading
from datetime import datetime

class MagicBricksScraper:
    def __init__(self, output_dir: str = "output"):
        self.session = requests.Session()
        # Ensure output folder exists so GitHub Actions can upload artifacts
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # Save files inside the output directory
        self.csv_filename = os.path.join(self.output_dir, 'magicbricks_projects.csv')
        self.json_filename = os.path.join(self.output_dir, 'magicbricks_projects.json')

        self.scraped_count = 0
        self.failed_count = 0
        self.start_time = None
        self.lock = threading.Lock()

        # Setup session headers
        self.session.headers.update({
            'User-Agent': 'MyMagicBricksScraper/1.0 (contact: your-email@example.com)',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })


    def getRera(self, pdp_url: str) -> str:
        """
        Extract RERA number from project details page
        """
        if not pdp_url:
            return ""
        
        # Construct full URL - FIXED: Remove extra slash
        full_url = f"https://www.magicbricks.com/{pdp_url}"
        
        try:
            response = self.session.get(full_url, timeout=10)
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find RERA ID element - multiple possible selectors
            rera_selectors = [
                'span.pdp__header--reraid__id',
                'span[class*="rera"]',
                'div[class*="rera"]',
                'span.rera-id',
                '.rera-number'
            ]
            
            rera_text = ""
            for selector in rera_selectors:
                rera_element = soup.select_one(selector)
                if rera_element:
                    rera_text = rera_element.get_text(strip=True)
                    if rera_text:
                        break
            
            # Additional fallback: look for text containing "RERA"
            if not rera_text:
                rera_elements = soup.find_all(string=lambda text: text and 'rera' in text.lower())
                for element in rera_elements:
                    if hasattr(element, 'parent'):
                        parent_text = element.parent.get_text(strip=True)
                        if 'rera' in parent_text.lower():
                            rera_text = parent_text
                            break
            
            return rera_text if rera_text else "Not Available"
            
        except requests.exceptions.Timeout:
            print(f"⏰ RERA timeout for: {pdp_url}")
            return "Timeout"
        except requests.exceptions.RequestException as e:
            print(f"🌐 RERA network error for {pdp_url}: {str(e)[:50]}...")
            return "Network Error"
        except Exception as e:
            print(f"❌ RERA error for {pdp_url}: {str(e)[:50]}...")
            return "Error"

    def getfloorPlan(self, unit_info: str) -> str:
        """
        Process unitInfo string to get unique floor plan values
        """
        if not unit_info:
            return ""
        
        try:
            unique_plans = set()
            for unit in unit_info.split('|'):
                if unit and ',' in unit:
                    plan_value = unit.split(',')[0].strip()
                    if plan_value:
                        unique_plans.add(plan_value)
            
            return ', '.join(sorted(unique_plans))
        except:
            return ""

    def write_to_csv(self, projects: List[Dict], mode: str = 'a'):
        """
        Write projects data to CSV file with error handling
        """
        if not projects:
            return False
        
        fieldnames = ['Name', 'Developer Name', 'Price Range', 'No of units', 
                     'Brochure', 'Total Acres', 'Floor Plan', 'RERA Number']
        
        try:
            with self.lock:
                with open(self.csv_filename, mode, newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    if mode == 'w' or csvfile.tell() == 0:
                        writer.writeheader()
                    
                    for project in projects:
                        writer.writerow(project)
            
            return True
        except Exception as e:
            print(f"❌ CSV Write Error: {e}")
            return False

    def print_progress(self, current_page: int, total_pages: int, page_projects: int, rera_stats: Dict):
        """
        Print detailed progress information with RERA statistics
        """
        elapsed_time = time.time() - self.start_time
        pages_per_minute = current_page / (elapsed_time / 60) if elapsed_time > 0 else 0
        estimated_total_time = (total_pages / current_page) * elapsed_time if current_page > 0 else 0
        remaining_time = estimated_total_time - elapsed_time
        
        rera_success = rera_stats.get('success', 0)
        rera_failed = rera_stats.get('failed', 0)
        rera_total = rera_success + rera_failed
        
        print(f"\n📊 Page {current_page}/{total_pages} | "
              f"Projects: {page_projects} | "
              f"Total: {self.scraped_count} | "
              f"Failed: {self.failed_count}")
        print(f"🏷️  RERA: {rera_success}✓ {rera_failed}✗ ({rera_total} total)")
        print(f"⏱️  Elapsed: {self.format_time(elapsed_time)} | "
              f"Speed: {pages_per_minute:.1f} pages/min | "
              f"ETA: {self.format_time(remaining_time)}")
        print("-" * 80)

    def format_time(self, seconds: float) -> str:
        """Format seconds to readable time"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.0f}m {seconds%60:.0f}s"
        else:
            return f"{seconds/3600:.0f}h {(seconds%3600)/60:.0f}m"

    def scrape_single_page(self, page_no: int) -> List[Dict]:
        """
        Scrape a single page with RERA scraping enabled
        """
        url = f"https://www.magicbricks.com/mbproject/newProjectCards?&pageNo={page_no}&city=3327"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            projects = []
            rera_stats = {'success': 0, 'failed': 0}
            
            if 'projectsCards' in data and data['projectsCards']:
                print(f"📄 Page {page_no}: Found {len(data['projectsCards'])} projects")
                
                for i, project in enumerate(data['projectsCards'], 1):
                    try:
                        project_data = {
                            'Name': project.get('psmName', ''),
                            'Developer Name': project.get('lmtDName', ''),
                            'Price Range': project.get('showPriceRange', ''),
                            'No of units': project.get('totalUnits', ''),
                            'Brochure': project.get('sblink', ''),
                            'Total Acres': project.get('projArea', ''),
                            'Floor Plan': self.getfloorPlan(project.get('unitInfo', '')),
                            'RERA Number': ''
                        }
                        
                        # Scrape RERA number for each project
                        pdp_url = project.get('pdpUrl', '')
                        if pdp_url:
                            print(f"   🔍 Getting RERA for project {i}/{len(data['projectsCards'])}...")
                            rera_number = self.getRera(pdp_url)
                            project_data['RERA Number'] = rera_number
                            
                            if rera_number and rera_number not in ['Not Available', 'Timeout', 'Network Error', 'Error']:
                                rera_stats['success'] += 1
                                print(f"   ✅ RERA found: {rera_number}")
                            else:
                                rera_stats['failed'] += 1
                                print(f"   ❌ RERA not available")
                            
                            # Respectful delay between RERA requests
                            time.sleep(1)
                        else:
                            project_data['RERA Number'] = "No PDP URL"
                            rera_stats['failed'] += 1
                        
                        projects.append(project_data)
                        self.scraped_count += 1
                        
                    except Exception as e:
                        self.failed_count += 1
                        print(f"❌ Project processing error: {e}")
                        continue
                
                return projects, rera_stats
            
            return [], rera_stats
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Page {page_no} request failed: {e}")
            return [], {'success': 0, 'failed': 0}
        except json.JSONDecodeError as e:
            print(f"❌ Page {page_no} JSON decode error: {e}")
            return [], {'success': 0, 'failed': 0}
        except Exception as e:
            print(f"❌ Page {page_no} unexpected error: {e}")
            return [], {'success': 0, 'failed': 0}

    def scrape_multiple_pages(self, start_page: int, end_page: int, batch_size: int = 20) -> List[Dict]:
        """
        Scrape multiple pages with RERA scraping and monitoring
        """
        print(f"🚀 Starting scraping from page {start_page} to {end_page}")
        print(f"⚙️  Batch size: {batch_size} | RERA scraping: ENABLED")
        print("⚠️  Note: RERA scraping will slow down the process significantly")
        
        self.start_time = time.time()
        all_projects = []
        current_batch = []
        total_rera_stats = {'success': 0, 'failed': 0}
        
        # Initialize CSV file
        if start_page == 1 and os.path.exists(self.csv_filename):
            backup_name = f"backup_{int(time.time())}_{self.csv_filename}"
            os.rename(self.csv_filename, backup_name)
            print(f"💾 Existing CSV backed up as: {backup_name}")
        
        for page_no in range(start_page, end_page + 1):
            try:
                # Scrape the page with RERA data
                page_projects, page_rera_stats = self.scrape_single_page(page_no)
                
                # Update RERA statistics
                total_rera_stats['success'] += page_rera_stats['success']
                total_rera_stats['failed'] += page_rera_stats['failed']
                
                if page_projects:
                    all_projects.extend(page_projects)
                    current_batch.extend(page_projects)
                    
                    # Write batch to CSV if reached batch size
                    if len(current_batch) >= batch_size:
                        mode = 'w' if page_no == start_page and len(all_projects) <= batch_size else 'a'
                        if self.write_to_csv(current_batch, mode):
                            print(f"💾 Batch written: {len(current_batch)} projects")
                        current_batch = []
                
                # Print progress every page
                self.print_progress(page_no, end_page, len(page_projects), page_rera_stats)
                
                # Small delay between pages to be respectful
                if page_no < end_page:
                    time.sleep(2)  # Increased delay for RERA scraping
                
            except KeyboardInterrupt:
                print(f"\n⏹️  Scraping interrupted by user at page {page_no}")
                break
            except Exception as e:
                print(f"❌ Critical error on page {page_no}: {e}")
                self.failed_count += 1
                continue
        
        # Write any remaining projects in the final batch
        if current_batch:
            mode = 'w' if not all_projects else 'a'
            if self.write_to_csv(current_batch, mode):
                print(f"💾 Final batch written: {len(current_batch)} projects")
        
        # Save to JSON as well
        try:
            with open(self.json_filename, 'w', encoding='utf-8') as f:
                json.dump(all_projects, f, indent=2, ensure_ascii=False)
            print(f"💾 JSON backup saved: {self.json_filename}")
        except Exception as e:
            print(f"❌ JSON save error: {e}")
        
        return all_projects, total_rera_stats

    def display_summary(self, total_rera_stats: Dict):
        """Display final summary with RERA statistics"""
        total_time = time.time() - self.start_time
        rera_success_rate = (total_rera_stats['success'] / (total_rera_stats['success'] + total_rera_stats['failed'])) * 100 if (total_rera_stats['success'] + total_rera_stats['failed']) > 0 else 0
        
        print(f"\n🎉 SCRAPING COMPLETED!")
        print("=" * 60)
        print(f"✅ Total projects scraped: {self.scraped_count}")
        print(f"❌ Failed projects: {self.failed_count}")
        print(f"🏷️  RERA Statistics:")
        print(f"   ✓ Successful: {total_rera_stats['success']}")
        print(f"   ✗ Failed: {total_rera_stats['failed']}")
        print(f"   📊 Success rate: {rera_success_rate:.1f}%")
        print(f"⏱️  Total time: {self.format_time(total_time)}")
        print(f"📊 Average speed: {self.scraped_count/(total_time/60):.1f} projects/min")
        print(f"💾 CSV file: {self.csv_filename}")
        print(f"📁 JSON file: {self.json_filename}")
        print("=" * 60)
        
        # Display CSV file info
    try:
        with open(self.csv_filename, 'r', encoding='utf-8') as f:
           reader = csv.reader(f)
           total_rows = sum(1 for _ in reader)
           row_count = total_rows - 1 if total_rows > 0 else 0  # Exclude header safely
           print(f"📈 CSV contains {row_count} records (excluding header)")
    except Exception as e:
         print("❌ Could not read CSV file for verification:", e)


def main():
    """Main execution function"""
    scraper = MagicBricksScraper()
    
    # Scrape 104 pages WITH RERA scraping
    try:
        projects, rera_stats = scraper.scrape_multiple_pages(
            start_page=1, 
            end_page=104, 
            batch_size=20
        )
        
        scraper.display_summary(rera_stats)
        
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
        # Still show summary for partial results
        scraper.display_summary({'success': 0, 'failed': 0})
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        scraper.display_summary({'success': 0, 'failed': 0})

if __name__ == "__main__":
    main()