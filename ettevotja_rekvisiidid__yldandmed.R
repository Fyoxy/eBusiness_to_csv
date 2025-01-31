library(data.table)
library(xml2)
library(readr)
library(dplyr)

fileName <- "ettevotja_rekvisiidid__yldandmed.xml"
output_file <- "ettevotja_rekvisiidid__yldandmed.csv"

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
    
    # Extract nested fields in <yldandmed>
    yldandmed <- xml_find_first(xml, ".//yldandmed")
    yldandmed_data <- data.table(
      ettevotteregistri_nr = xml_text(xml_find_first(yldandmed, "./ettevotteregistri_nr")),
      esmaregistreerimise_kpv = xml_text(xml_find_first(yldandmed, "./esmaregistreerimise_kpv")),
      kustutamise_kpv = xml_text(xml_find_first(yldandmed, "./kustutamise_kpv")),
      staatus = xml_text(xml_find_first(yldandmed, "./staatus")),
      staatus_tekstina = xml_text(xml_find_first(yldandmed, "./staatus_tekstina")),
      piirkond = xml_text(xml_find_first(yldandmed, "./piirkond")),
      piirkond_tekstina = xml_text(xml_find_first(yldandmed, "./piirkond_tekstina")),
      piirkond_tekstina_pikk = xml_text(xml_find_first(yldandmed, "./piirkond_tekstina_pikk")),
      evks_registreeritud = xml_text(xml_find_first(yldandmed, "./evks_registreeritud")),
      evks_registreeritud_kande_kpv = xml_text(xml_find_first(yldandmed, "./evks_registreeritud_kande_kpv")),
      oiguslik_vorm = xml_text(xml_find_first(yldandmed, "./oiguslik_vorm")),
      oiguslik_vorm_nr = xml_text(xml_find_first(yldandmed, "./oiguslik_vorm_nr")),
      oiguslik_vorm_tekstina = xml_text(xml_find_first(yldandmed, "./oiguslik_vorm_tekstina")),
      oigusliku_vormi_alaliik = xml_text(xml_find_first(yldandmed, "./oigusliku_vormi_alaliik")),
      lahknevusteade_puudumisest = xml_text(xml_find_first(yldandmed, "./lahknevusteade_puudumisest")),
      oigusliku_vormi_alaliik_tekstina = xml_text(xml_find_first(yldandmed, "./oigusliku_vormi_alaliik_tekstina")),
      asutatud_sissemakset_tegemata = xml_text(xml_find_first(yldandmed, "./asutatud_sissemakset_tegemata")),
      loobunud_vorminouetest = xml_text(xml_find_first(yldandmed, "./loobunud_vorminouetest")),
      on_raamatupidamiskohustuslane = xml_text(xml_find_first(yldandmed, "./on_raamatupidamiskohustuslane")),
      tegutseb = xml_text(xml_find_first(yldandmed, "./tegutseb")),
      tegutseb_tekstina = xml_text(xml_find_first(yldandmed, "./tegutseb_tekstina")),
      esitab_kasusaajad = xml_text(xml_find_first(yldandmed, "./esitab_kasusaajad"))
    )
    
    # Extract other nested fields (<arinimed>, <aadressid>, <kapitalid>, <tegevusalad>, <aruannetest>)
    staatused_items <- xml_find_all(xml, ".//staatused/item")
    staatused_table <- rbindlist(lapply(staatused_items, function(item) {
      data.table(
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        staatus = xml_text(xml_find_first(item, "./staatus")),
        staatus_tekstina = xml_text(xml_find_first(item, "./staatus_tekstina")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv"))
      )
    }))
    
    # Extract other nested fields (<arinimed>, <aadressid>, <kapitalid>, <tegevusalad>, <aruannetest>)
    arinimed_items <- xml_find_all(xml, ".//arinimed/item")
    arinimed_table <- rbindlist(lapply(arinimed_items, function(item) {
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
    
    aadressid_items <- xml_find_all(xml, ".//aadressid/item")
    aadressid_table <- rbindlist(lapply(aadressid_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        riik = xml_text(xml_find_first(item, "./riik")),
        riik_tekstina = xml_text(xml_find_first(item, "./riik_tekstina")),
        ehak = xml_text(xml_find_first(item, "./ehak")),
        ehak_nimetus = xml_text(xml_find_first(item, "./ehak_nimetus")),
        tanav_maja_korter = xml_text(xml_find_first(item, "./tanav_maja_korter")),
        aadress_ads__ads_oid = xml_text(xml_find_first(item, "./aadress_ads__ads_oid")),
        aadress_ads__adr_id = xml_text(xml_find_first(item, "./aadress_ads__adr_id")),
        aadress_ads__ads_normaliseeritud_taisaadress = xml_text(xml_find_first(item, "./aadress_ads__ads_normaliseeritud_taisaadress")),
        aadress_ads__ads_normaliseeritud_taisaadress_tapsustus = xml_text(xml_find_first(item, "./aadress_ads__ads_normaliseeritud_taisaadress_tapsustus")),
        aadress_ads__koodaadress = xml_text(xml_find_first(item, "./aadress_ads__koodaadress")),
        aadress_ads__adob_id = xml_text(xml_find_first(item, "./aadress_ads__adob_id")),
        aadress_ads__tyyp = xml_text(xml_find_first(item, "./aadress_ads__tyyp")),
        postiindeks = xml_text(xml_find_first(item, "./postiindeks")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    oiguslikud_vormid_items <- xml_find_all(xml, ".//oiguslikud_vormid/item")
    oiguslikud_vormid_table <- rbindlist(lapply(oiguslikud_vormid_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        sisu = xml_text(xml_find_first(item, "./sisu")),
        sisu_nr = xml_text(xml_find_first(item, "./sisu_nr")),
        sisu_tekstina = xml_text(xml_find_first(item, "./sisu_tekstina")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    kapitalid_items <- xml_find_all(xml, ".//kapitalid/item")
    kapitalid_table <- rbindlist(lapply(kapitalid_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        kapitali_suurus = xml_text(xml_find_first(item, "./kapitali_suurus")),
        kapitali_valuuta = xml_text(xml_find_first(item, "./kapitali_valuuta")),
        kapitali_valuuta_tekstina = xml_text(xml_find_first(item, "./kapitali_valuuta_tekstina")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    majandusaastad_items <- xml_find_all(xml, ".//majandusaastad/item")
    majandusaastad_table <- rbindlist(lapply(majandusaastad_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        maj_aasta_algus = xml_text(xml_find_first(item, "./maj_aasta_algus")),
        maj_aasta_lopp = xml_text(xml_find_first(item, "./maj_aasta_lopp")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    pohikirjad_items <- xml_find_all(xml, ".//pohikirjad/item")
    pohikirjad_table <- rbindlist(lapply(pohikirjad_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        kinnitamise_kpv = xml_text(xml_find_first(item, "./kinnitamise_kpv")),
        muutmise_kpv = xml_text(xml_find_first(item, "./muutmise_kpv")),
        selgitus = xml_text(xml_find_first(item, "./selgitus")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv")),
        sisaldab_erioigusi = xml_text(xml_find_first(item, "./sisaldab_erioigusi"))
      )
    }))
    
    markused_kaardil_items <- xml_find_all(xml, ".//markused_kaardil/item")
    markused_kaardil_table <- rbindlist(lapply(markused_kaardil_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr")),
        veerg_nr = xml_text(xml_find_first(item, "./veerg_nr")),
        tyyp = xml_text(xml_find_first(item, "./tyyp")),
        tyyp_tekstina = xml_text(xml_find_first(item, "./tyyp_tekstina")),
        sisu = xml_text(xml_find_first(item, "./sisu")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    sidevahendid_items <- xml_find_all(xml, ".//sidevahendid/item")
    sidevahendid_table <- rbindlist(lapply(sidevahendid_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        liik = xml_text(xml_find_first(item, "./liik")),
        liik_tekstina = xml_text(xml_find_first(item, "./liik_tekstina")),
        sisu = xml_text(xml_find_first(item, "./sisu")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv")),
        kaardi_piirkond = xml_text(xml_find_first(item, "./kaardi_piirkond")),
        kaardi_nr = xml_text(xml_find_first(item, "./kaardi_nr")),
        kaardi_tyyp = xml_text(xml_find_first(item, "./kaardi_tyyp")),
        kande_nr = xml_text(xml_find_first(item, "./kande_nr"))
      )
    }))
    
    # Extract nested <teatatud_tegevusalad>/<item> elements (business activities)
    tegevusalad_items <- xml_find_all(xml, ".//teatatud_tegevusalad/item")
    tegevusalad_table <- rbindlist(lapply(tegevusalad_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        emtak_kood = xml_text(xml_find_first(item, "./emtak_kood")),
        emtak_tekstina = xml_text(xml_find_first(item, "./emtak_tekstina")),
        emtak_versioon = xml_text(xml_find_first(item, "./emtak_versioon")),
        nace_kood = xml_text(xml_find_first(item, "./nace_kood")),
        on_pohitegevusala = xml_text(xml_find_first(item, "./on_pohitegevusala")),
        algus_kpv = xml_text(xml_find_first(item, "./algus_kpv")),
        lopp_kpv = xml_text(xml_find_first(item, "./lopp_kpv"))
      )
    }))
    
    # Extract nested <info_majandusaasta_aruannetest>/<item> elements (annual reports)
    aruannetest_items <- xml_find_all(xml, ".//info_majandusaasta_aruannetest/item")
    aruannetest_table <- rbindlist(lapply(aruannetest_items, function(item) {
      data.table(
        kirje_id = xml_text(xml_find_first(item, "./kirje_id")),
        majandusaasta_perioodi_algus_kpv = xml_text(xml_find_first(item, "./majandusaasta_perioodi_algus_kpv")),
        majandusaasta_perioodi_lopp_kpv = xml_text(xml_find_first(item, "./majandusaasta_perioodi_lopp_kpv")),
        tootajate_arv = xml_text(xml_find_first(item, "./tootajate_arv")),
        ettevotja_aadress_aruandes = xml_text(xml_find_first(item, "./ettevotja_aadress_aruandes")),
        aruande_kpv = xml_text(xml_find_first(item, "./aruande_kpv"))
      )
    }))
    
    # Combine all the data tables into one list
    final_data <- list(
      yldandmed_data = yldandmed_data,
      staatused_table = staatused_table,
      arinimed_table = arinimed_table,
      aadressid_table = aadressid_table,
      oiguslikud_vormid_table = oiguslikud_vormid_table,
      kapitalid_table = kapitalid_table,
      majandusaastad_table = majandusaastad_table,
      pohikirjad_table = pohikirjad_table,
      markused_kaardil_table = markused_kaardil_table,
      sidevahendid_table = sidevahendid_table,
      tegevusalad_table = tegevusalad_table,
      aruannetest_table = aruannetest_table
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
