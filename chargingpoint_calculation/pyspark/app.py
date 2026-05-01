import sys
from pipeline import PySparkJob

def main():
    job = PySparkJob()

    observed = job.read_csv(sys.argv[1])
    required = job.read_csv(sys.argv[2])
    print("print observed schema")
    observed.printSchema()
    required.printSchema()

    print("<<Avg Observed Temperature>>")
    avg_observed_temp = job.calc_average_temperature(observed)
    avg_observed_temp.show()

    print("<<Faulty Plants>>")
    faulty_plants = job.find_faulty_plants(avg_observed_temp, required)
    faulty_plants.show()

    job.save_as(faulty_plants, sys.argv[3])

if __name__ == "__main__":
    main()
