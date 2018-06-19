version 1.0

task dont_volunteer_to_pay_localization_task{
    input {
        File input_file
    }
    command {
        cat ~{input_file}
    }

    output {
        File out = stdout()
    }

    runtime {
        docker: "us.gcr.io/google-containers/ubuntu-slim:0.14"
    }
}


workflow dont_volunteer_to_pay_localization {
    File workflow_input
    call dont_volunteer_to_pay_localization_task {
        input: input_file = workflow_input
    }
}
