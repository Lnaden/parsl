import sys
import pickle
from ipyparallel.serialize import unpack_apply_message, serialize_object


def check_file(parsl_file_obj, mapping, file_type_string):
    type_desc = str(type(parsl_file_obj))
    # Rename the file type string to the appropriate string
    if file_type_string is None:
        file_type_string = "<class 'parsl.data_provider.files.File'>"
    # Obtain the local_path from the mapping
    if type_desc == file_type_string:
        if parsl_file_obj.filepath in mapping:
            parsl_file_obj.local_path = mapping[parsl_file_obj.filepath]


if __name__ == "__main__":
    name = "parsl"
    shared_fs = False
    input_function_file = ""
    output_result_file = ""
    remapping_string = None
    file_type_string = None

    # Parse command line options
    try:
        index = 1
        while index < len(sys.argv):
            if sys.argv[index] == "-i":
                input_function_file = sys.argv[index + 1]
                index += 1
            elif sys.argv[index] == "-o":
                output_result_file = sys.argv[index + 1]
                index += 1
            elif sys.argv[index] == "-r":
                remapping_string = sys.argv[index + 1]
                index += 1
            elif sys.argv[index] == "-t":
                file_type_string = sys.argv[index + 1]
                index += 1
            elif sys.argv[index] == "--shared-fs":
                shared_fs = True
            else:
                print("command line argument not supported")
                exit(1)
            index += 1
    except Exception as e:
        print(e)
        exit(1)

    # Load function data
    try:
        input_function = open(input_function_file, "rb")
        function_tuple = pickle.load(input_function)
        input_function.close()
    except Exception as e:
        print(e)
        exit(2)

    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})
    f, args, kwargs = unpack_apply_message(function_tuple, user_ns, copy=False)

    # Remapping file names using remapping string
    mapping = {}
    try:
        if shared_fs is False and remapping_string is not None:
            # Parse the remapping string into a dictionary
            for i in remapping_string.split(","):
                split_mapping = i.split(":")
                mapping[split_mapping[0]] = split_mapping[1]

            # Check file attributes for inputs and make appropriate changes
            func_inputs = kwargs.get("inputs", [])
            for inp in func_inputs:
                check_file(inp, mapping, file_type_string)

            # Iterate through all arguments to the function
            for kwarg, potential_f in kwargs.items():
                # Process the "stdout" and "stderr" arguments and add them to kwargs
                if kwarg == "stdout" or kwarg == "stderr":
                    if (isinstance(potential_f, str) or isinstance(potential_f, tuple)):
                        if isinstance(potential_f, tuple) and len(potential_f) == 2:
                            l_f = list(potential_f)
                            p_f = l_f[0]
                            if p_f in mapping:
                                p_f = mapping[p_f]
                            kwargs[kwarg] = tuple([p_f] + l_f[1:])
                        elif isinstance(potential_f, str):
                            if potential_f in mapping:
                                kwargs[kwarg] = mapping[potential_f]
                else:
                    check_file(potential_f, mapping, file_type_string)

            # Check all non-key-word arguments and make appropriate changes
            for inp in args:
                check_file(inp, mapping, file_type_string)

            # Check file attributes for outputs and make appropriate changes
            func_outputs = kwargs.get("outputs", [])
            for output in func_outputs:
                check_file(output, mapping, file_type_string)
    except Exception as e:
        print(e)
        exit(3)

    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    # Add variables to the namespace to make function call
    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    # Perform the function call and handle errors
    try:
        exec(code, user_ns, user_ns)
    # Failed function execution
    except Exception as e:
        print(e)
        exec_info = sys.exc_info()
        result_package = {"failure": True, "result": serialize_object(exec_info)}
    # Successful function execution
    else:
        result = user_ns.get(resultname)
        result_package = {"failure": False, "result": serialize_object(result)}

    # Write out function result to the result file
    try:
        f = open(output_result_file, "wb")
        pickle.dump(result_package, f)
        f.close()
        exit(0)
    except Exception as e:
        print(e)
        exit(4)
