// Provider/Organization aliases for medical fraud database
// This maps common variations of provider names to canonical forms

export const POWER_ALIASES = {
  // Example aliases - add your own based on your data
  // Format: "Canonical Name": ["alias1", "alias2", "alias3"]
  
  // Healthcare Systems
  "HCA Healthcare": ["HCA", "Hospital Corporation of America"],
  "Tenet Healthcare": ["Tenet", "Tenet Health System"],
  "Community Health Systems": ["CHS", "Community Health"],
  "Universal Health Services": ["UHS", "Universal Health"],
  
  // Major Hospitals
  "Mayo Clinic": ["Mayo"],
  "Cleveland Clinic": ["Cleveland"],
  "Johns Hopkins": ["Johns Hopkins Hospital", "JHH"],
  
  // Pharma Companies
  "Johnson & Johnson": ["J&J", "JNJ", "Janssen"],
  "Pfizer": ["Pfizer Inc"],
  "Merck": ["Merck & Co"],
  "AbbVie": ["Abbott"],
  
  // Device Manufacturers
  "Medtronic": ["Medtronic PLC"],
  "Boston Scientific": ["BSX"],
  "Stryker": ["Stryker Corporation"],
  
  // Lab Companies
  "Quest Diagnostics": ["Quest"],
  "LabCorp": ["Laboratory Corporation of America"],
  
  // Pharmacy Chains
  "CVS Health": ["CVS", "CVS Pharmacy", "CVS Caremark"],
  "Walgreens": ["Walgreens Boots Alliance", "WBA"],
  "Rite Aid": ["Rite Aid Corporation"],
  
  // Add more as you discover them in your data
};