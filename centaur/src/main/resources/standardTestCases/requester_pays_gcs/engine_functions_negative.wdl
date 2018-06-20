version 1.0

workflow requester_pays_engine_functions_negative {
    File input_file = "gs://cromwell_bucket_with_requester_pays/lorem ipsum.txt"
    call functions {
        input: input_string = read_string(input_file),
               input_size = size(input_file)
    }
    output {
        String result = functions.o
    }
}


task functions {
    input {
        String input_string
        Float input_size
    }
    command {
        echo ~{input_string}
        echo ~{input_size}
    }
    runtime {
        backend: "Papiv2"
        docker: "ubuntu"
    }
    output {
        String o = read_string(stdout())
    }
}
