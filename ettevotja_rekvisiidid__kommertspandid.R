library(data.table)
library(xml2)
library(readr)
library(dplyr)

fileName <- "ettevotja_rekvisiidid__kommertspandid.xml"
output_file <- "ettevotja_rekvisiidid__kommertspandid.csv"

# Number of sections to process (for example, limit to first 10 sections)
batch_amount <- 1000
total_records <- 4925
company_counter <- 0

process_extracted_lines <- function(extracted_lines) {
  # If the section has been fully extracted, process and extract data
  if (length(extracted_lines) > 0) {
    # Combine the extracted lines into a single string
    extracted_content <- paste(extracted_lines, collapse = "\n")
    
    # Parse the XML and extract data
    xml <- read_xml(extracted_content)
    
    # Extract top-level fields
    ariregistri_kood <- xml_text(xml_find_first(xml, "./ariregistri_kood"))
    nimi <- xml_text(xml_find_first(xml, "./nimi"))
    
    kommertspandiandmed <- xml_find_all(xml, ".//kommertspandiandmed/item")
    kommertspandiandmed_data <- data.table(
      kirje_id = xml_text(xml_find_first(kommertspandiandmed, "./kirje_id")),
      kaardi_piirkond = xml_text(xml_find_first(kommertspandiandmed, "./kaardi_piirkond")),
      kaardi_nr = xml_text(xml_find_first(kommertspandiandmed, "./kaardi_nr")),
      kaardi_tyyp = xml_text(xml_find_first(kommertspandiandmed, "./kaardi_tyyp")),
      kande_nr = xml_text(xml_find_first(kommertspandiandmed, "./kande_nr")),
      pandi_id = xml_text(xml_find_first(kommertspandiandmed, "./pandi_id")),
      pandi_number = xml_text(xml_find_first(kommertspandiandmed, "./pandi_number")),
      pandi_olek = xml_text(xml_find_first(kommertspandiandmed, "./pandi_olek")),
      olek_tekstina = xml_text(xml_find_first(kommertspandiandmed, "./olek_tekstina")),
      pandi_jarjekoht = xml_text(xml_find_first(kommertspandiandmed, "./pandi_jarjekoht")),
      pandi_jarjekoht_tekstina = xml_text(xml_find_first(kommertspandiandmed, "./pandi_jarjekoht_tekstina")),
      pandi_summa = xml_text(xml_find_first(kommertspandiandmed, "./pandi_summa")),
      pandi_valuuta = xml_text(xml_find_first(kommertspandiandmed, "./pandi_valuuta")),
      pandi_valuuta_tekstina = xml_text(xml_find_first(kommertspandiandmed, "./pandi_valuuta_tekstina")),
      algus_kpv = xml_text(xml_find_first(kommertspandiandmed, "./algus_kpv")),
      lopp_kpv = xml_text(xml_find_first(kommertspandiandmed, "./lopp_kpv"))
    )
    
    jarjekohad_items <- xml_find_all(xml, ".//jarjekohad/item")
    jarjekohad_table <- rbindlist(lapply(jarjekohad_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        sisu = xml_text(xml_find_first(item, "./sisu")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    pandisummad_items <- xml_find_all(xml, ".//pandisummad/item")
    pandisummad_table <- rbindlist(lapply(pandisummad_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        summa = xml_text(xml_find_first(item, "./summa")),
        valuuta = xml_text(xml_find_first(item, "./valuuta")),
        valuuta_tekstina = xml_text(xml_find_first(item, "./valuuta_tekstina")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    pandipidajad_items <- xml_find_all(xml, ".//pandipidajad/item")
    pandipidajad_table <- rbindlist(lapply(pandipidajad_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        pidaja_isikuliik = xml_text(xml_find_first(item, "./pidaja_isikuliik")),
        pidaja_nimi = xml_text(xml_find_first(item, "./pidaja_nimi")),
        pidaja_kood = xml_text(xml_find_first(item, "./pidaja_kood")),
        pidaja_riik = xml_text(xml_find_first(item, "./pidaja_riik")),
        pidaja_riik_tekstina = xml_text(xml_find_first(item, "./pidaja_riik_tekstina")),
        pidaja_asukoht_ehak = xml_text(xml_find_first(item, "./pidaja_asukoht_ehak")),
        pidaja_asukoht_ehak_tekstina = xml_text(xml_find_first(item, "./pidaja_asukoht_ehak_tekstina")),
        pidaja_murdosa = xml_text(xml_find_first(item, "./pidaja_murdosa")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    # Combine all the data tables into one list
    final_data <- list(
      kommertspandiandmed_data = kommertspandiandmed_data, 
      jarjekohad_table = jarjekohad_table, 
      pandisummad_table = pandisummad_table, 
      pandipidajad_table = pandipidajad_table
    )
    
    # Add a source column to each data table
    final_data_with_sources <- lapply(names(final_data), function(tbl_name) {
      data <- final_data[[tbl_name]]
      data$source <- tbl_name
      data$ariregistri_kood <- ariregistri_kood
      data$nimi <- nimi
      return(data)
    })
    
    # Combine all data frames into one
    final_combined_data <- bind_rows(final_data_with_sources)
    
    # Return the combined data
    return(final_combined_data)
  } else {
    return(NULL)
  }
}

final_combined_data <- NULL  # Initialize final combined data table
is_inside_section <- FALSE
extracted_lines <- c()


# Read the next 1000 lines
print("Reading data...")
lines <- read_lines(fileName)

print("Processing data...")
# Iterate through the lines and extract relevant sections
for (line in lines) {
  
  if (grepl("</ettevotjad>", line)) {
    # File complete, write all data and quit
    setcolorder(empty_object, c("ariregistri_kood", "nimi"))
    write_csv(empty_object, output_file, append = TRUE)
    break
  }
  
  if (grepl("<ettevotja>", line)) {
    is_inside_section <- TRUE
  }
  
  if (is_inside_section) {
    extracted_lines <- c(extracted_lines, line)
  }
  
  if (grepl("</ettevotja>", line)) {
    company_counter <- 1 + company_counter
    
    is_inside_section <- FALSE
    
    final_data <- process_extracted_lines(extracted_lines)
    
    # Check if being ran for the first time
    if (company_counter == 1) {
      # Initialize main object with data
      empty_object <- final_data
    }
    else {
      
      # Check if being ran for the 1000th time
      if (company_counter %% batch_amount == 0) {
        percentage_processed <- (company_counter / total_records) * 100
        cat(sprintf("Processed %d/%d records (%.2f%%)\n", company_counter, total_records, percentage_processed))
        
        # Export data and refresh object
        setcolorder(empty_object, c("ariregistri_kood", "nimi"))
        write_csv(empty_object, output_file, append = TRUE)
        
        # Resetting object
        empty_object <- final_data
      }
      else {
        # Add data to object
        empty_object <- rbind(empty_object,final_data, fill = TRUE)
      }
    }
    
    # Reset the extracted lines for the next section
    extracted_lines <- c()
  }
}

percentage_processed <- (company_counter / total_records) * 100
cat(sprintf("Processed %d/%d records (%.2f%%)\n", company_counter, total_records, percentage_processed))
print("DONE!")
