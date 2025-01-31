library(data.table)
library(xml2)
library(readr)
library(dplyr)

fileName <- "ettevotja_rekvisiidid__maarused.xml"
output_file <- "ettevotja_rekvisiidid__maarused.csv"

# Number of sections to process (for example, limit to first 10 sections)
batch_amount <- 1000
total_records <- 347622
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
    
    maarused_items <- xml_find_all(xml, ".//maarused/item")
    maarused_table <- rbindlist(lapply(maarused_items, function(item) {
      data.table(
        maaruse_nr = xml_text(xml_find_first(item, "./maaruse_nr")),
        maaruse_kpv = xml_text(xml_find_first(item, "./maaruse_kpv")),
        kande_kpv = xml_text(xml_find_first(item, "./kande_kpv")),
        lisatahtaeg = xml_text(xml_find_first(item, "./lisatahtaeg")),
        maaruse_liik = xml_text(xml_find_first(item, "./maaruse_liik")),
        maaruse_liik_tekstina = xml_text(xml_find_first(item, "./maaruse_liik_tekstina")),
        maaruse_olek = xml_text(xml_find_first(item, "./maaruse_olek")),
        maaruse_olek_tekstina = xml_text(xml_find_first(item, "./maaruse_olek_tekstina")),
        kandeliik = xml_text(xml_find_first(item, "./kandeliik")),
        kandeliik_tekstina = xml_text(xml_find_first(item, "./kandeliik_tekstina")),
        joustumise_kpv = xml_text(xml_find_first(item, "./joustumise_kpv")),
        joust_olek = xml_text(xml_find_first(item, "./joust_olek")),
        joust_olek_tekstina = xml_text(xml_find_first(item, "./joust_olek_tekstina"))
      )
    }))
    
    # Combine all the data tables into one list
    final_data <- list(
      maarused_table = maarused_table
    )
    
    # Add a source column to each data table
    final_data_with_sources <- lapply(names(final_data), function(tbl_name) {
      data <- final_data[[tbl_name]]
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
