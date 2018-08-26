#include <bits/stdc++.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <thread>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <random>
#include <time.h>
#include <mutex>
#include <atomic>

using namespace std;

atomic <int> total{0};
atomic <int> check_sleep{0};
atomic <int> tuples_counter{0};

FILE *fp;
void error(const char* msg)
{
	perror(msg);
	exit(1);
}

// returns the exponential decay for lambda
double run_exp(float lambda)
{
    default_random_engine generate;

    exponential_distribution <double> distribution(1.0/lambda);

    return distribution(generate);
}

void receiver(int index, int max_clientSize, std::vector <int> nodes, int vClock[], int size_vClock, std::mutex *mtx, int m, int lastSent[], int lastUpdate[])
{
	

		int PORT = 3000+index;
		
		int server_fd, new_socket, valread;
	    struct sockaddr_in address;

	    int opt = 1;

	    int addrlen = sizeof(address);

	    char hello[] = "Hello from receiver";
	      
	    // Socket file descriptors are created here.
	    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
	    {
	        perror("socket failed");
	        exit(EXIT_FAILURE);
	    }
	      


	    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
	                                                  &opt, sizeof(opt)))
	    {
	        perror("setsockopt");
	        exit(EXIT_FAILURE);
	    }

	    address.sin_family = AF_INET;

	    address.sin_addr.s_addr = INADDR_ANY;

	    address.sin_port = htons( PORT );
	      
	    // The sockets are binded to the Port
	    if (bind(server_fd, (struct sockaddr *)&address, 
	                                 sizeof(address))<0)
	    {

	        perror("bind failed");

	        exit(EXIT_FAILURE);
	    }

	    // sockets starts listening for incoming connection
	    if (listen(server_fd, 4) < 0)
	    {
	        perror("listen");

	        exit(EXIT_FAILURE);
	    }

	    check_sleep--;

	    vector <int> client_list;
	    int max_client = max_clientSize;
	    int count = 0;

	    while(count < max_client)
	    {

	    	// sockets accepts incoming connections
	    	if ((new_socket = accept(server_fd, (struct sockaddr *)&address, 
	                       (socklen_t*)&addrlen))<0)
	    	{

	       		perror("accept");
	        	exit(EXIT_FAILURE);
	    	}


	    	else
	    	{
	    		//connected sockets are added to client list.
	    		client_list.push_back(new_socket);
	    		count++;
	    	}
	    }
	    fprintf(fp,"All connection established for client = %d\n", index);

	    char buffer[5000] = {0};
	    time_t now = time(0);

	    while(total>0)
	    {

	    	for(int i=0; i<max_client ;i++)
	    	{
	    		bzero(buffer,5000);
	    		
	    		if(total == 0)
	    		{
	    			fprintf(fp,"\nreturned receiver index = %d\n", index);
	    			return;
	    		}

	    		valread = read(client_list[i] , buffer, 5000);
	    		
		    	if(valread < 0)
		    	{
		    		
		    		error("error on reading..");
		    	}
		    	else
		    	{
		    		if(strlen(buffer)>0)
		    		{

			    		mtx->lock();
				    	std::vector<int> temp;
				    	int pos=-1,mssg_from;
			    		bool escape1 = false;

			    		int col_pos=0;
			    		int pointer_print = 0;

				    	while(col_pos<strlen(buffer))
				    	{
				    		while(buffer[col_pos] != ':')
				    		{
				    			col_pos++;
				    			if(col_pos == strlen(buffer))
				    			{
				    				escape1 = true;
				    				break;
				    			}		
				    		}
				    		if(escape1 == true)
				    			break;

				    		pos = col_pos;
				    		col_pos++;

				    		int temp_pos;
					    	if(buffer[pos-3] == ' ')
					    	{
					    		temp_pos = pos-4;
					    		mssg_from = (int)buffer[pos-2]-48;
					    	}
					    	else
					    	{
					    		temp_pos = pos-5;
					    		mssg_from = ((int)buffer[pos-3]-48)*10 + (int)buffer[pos-2]-48;		    		
					    	}

					    	if(buffer[temp_pos-1] == 'd')
					    		pointer_print = (int)buffer[temp_pos]-48;
					    	else
					    		pointer_print = ((int)buffer[temp_pos-1]-48)*10 + (int)buffer[temp_pos]-48;

					    
					    	total--;
					    	int index1=1,val=0,ptr;
					    	int temp_vClock[size_vClock+1];
					    	


					    	for(ptr = pos+2;;ptr++)
					    	{
					    		if(buffer[ptr] == '|')
					    		{
					    			
					    			break;
					    		}
					    		if(buffer[ptr] == ' ')
					    		{
					    			temp_vClock[index1] = val;
					    			val=0;
					    			index1++;
					    		}
					    		else
					    			val = val*10 + (int)buffer[ptr]-48;
					    	}
					    	
					    	std::string tuples = "";

					    	vClock[index]++;
					    	lastUpdate[index]++;
					    	//printf("here1 ptr = %d\n", ptr);
					    	while(ptr<strlen(buffer) && buffer[ptr] != '#')					    	
					    	{
					    		//printf("here\n");
					    		ptr++;
					    		if(ptr<strlen(buffer) && buffer[ptr] == '(')
					    		{
					    			ptr++;
					    			val=0;
					    			int temp_a,temp_b;
					    			while(buffer[ptr] != ',')
					    			{
					    				val = val*10 + (int)buffer[ptr]-48;
					    				ptr++;
					    			}
					    			temp_a = val;
					    			ptr++;
					    			val=0;

					    			while(buffer[ptr] != ')')
					    			{
					    				val = val*10 + (int)buffer[ptr]-48;
					    				ptr++;
					    			}
					    			temp_b = val;
					    			tuples +="("+std::to_string(temp_a)+", "+std::to_string(temp_b)+") ";
					    			
					    			if(vClock[temp_a] < temp_b)
					    			{
					    				
					    				vClock[temp_a] = temp_b;
					    				lastUpdate[temp_a] = vClock[index];
					    			}
					    		}			
					    	}

					    	char tuples_arr[tuples.size()+1];
					    	strcpy(tuples_arr,tuples.c_str());

					    	std::string printing_vClock="[";
					    	for(int i=1;i<=size_vClock;i++){
					    		if(i!=size_vClock)
					    			printing_vClock+=std::to_string(vClock[i])+" ";
					    		else
					    			printing_vClock+=std::to_string(vClock[i])+"]";
					    	}
					    	char cstr[printing_vClock.size()+1];
							strcpy(cstr,printing_vClock.c_str());

					    	tm *ltm = localtime(&now);
							fprintf(fp,"Process%d receives m%d%d from process%d at %d:%d,tuples received = : %s , updated vc = %s\n",index,mssg_from,pointer_print,mssg_from,ltm->tm_min,ltm->tm_sec,tuples_arr,cstr);
	
				    	}
			   			mtx->unlock();
			   		}
		    	}
		    	
	    	}
	    	
	    	if(total == 0)
		    {
		    	fprintf(fp,"receiver to exit= %d\n", index);
		    	break;
		    }
	    	
	    }
	    fprintf(fp,"Exiting receiver = %d\n", index);
	    return;
}

void sender(int index, std::vector <int> nodes, int n, int vClock[], std::mutex *mtx,int m, int lastSent[], int lastUpdate[], int lambda, int alpha1, int alpha2)
{

	while(check_sleep > 0);
	int max_connection = nodes.size();
	
	struct sockaddr_in address;
    int sock[max_connection], valread;
    //struct sockaddr_in serv_addr[max_connection];

    for(int i = 0;i<max_connection;i++)
    {
    	int PORT = 3000+nodes[i];
    
	    struct sockaddr_in serv_addr;

	    char buffer[5000] = {0};

	    if ((sock[i] = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	    {
	        printf("\n Socket creation error \n");
	    }
	  
	    memset(&serv_addr, '0', sizeof(serv_addr));
	  
	    serv_addr.sin_family = AF_INET;

	    serv_addr.sin_port = htons(PORT);
	      
	    // Convert IPv4 and IPv6 addresses from text to binary form

	    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) 
	    {
	        printf("\nInvalid address/ Address not supported \n");
	    }

	  
	    if (connect(sock[i], (struct sockaddr *)&serv_addr, sizeof(serv_addr))<0)
	    {
	        printf("\nConnection Failed index = %d\n",index);
	    }
    }

    int count = 0;
    int counter = 0;
    time_t now = time(0);
    while(count < m)
    {	
    	mtx->lock();
    	
    	vClock[index] = vClock[index]+1;
    	lastUpdate[index] = vClock[index];

    	std::string printing_vClock="[";
    	for(int i=1;i<=n;i++){
    		if(i!=n)
    			printing_vClock+=std::to_string(vClock[i])+" ";
    		else
    			printing_vClock+=std::to_string(vClock[i])+"]";
    	}
    	char cstr[printing_vClock.size()+1];
		strcpy(cstr,printing_vClock.c_str());

		counter++;
		int internal_counter = 0;
		int max_internal = (int)((double)alpha1/(double)(alpha2-alpha1)) * m;
    	//cout << "max = " << max_internal<<endl;
    	int random = rand()%alpha2+1;
    	if(random <= alpha1)
    	{
    		internal_counter++;
    		tm *ltm = localtime(&now);
    		fprintf(fp,"Process%d executes internal event e%d%d at %d:%d,vc: %s\n",index,index,counter,ltm->tm_min,ltm->tm_sec,cstr);	
    	}
    	else
    	{
    		count++;
    		std::string data="";
    		for(int i=1;i<=n;i++)
    		{
    			
    			data+=std::to_string(vClock[i])+" ";
    			
    		}
    		data+="|";
    		string tuples_send = "";
    		int random_index = rand()%max_connection;

    		for(int z=1;z<=n;z++)
    		{
    			if(lastSent[random_index+1] < lastUpdate[z])
    			{
    				tuples_counter++;
    				data+= "("+std::to_string(z)+","+std::to_string(vClock[z])+")";
    				tuples_send += "("+std::to_string(z)+","+std::to_string(vClock[z])+")";
    			}
    		}
    		data+="#";

    		char tuples_send_arr[tuples_send.size()+1];
			strcpy(tuples_send_arr,tuples_send.c_str());

		   	std::string s = "Hello from send"+ std::to_string(counter)+ " " +std::to_string(index)+" : "+data;
		   	char hello[s.length()];
		   	strcpy(hello, s.c_str());

		   	tm *ltm = localtime(&now);
			fprintf(fp,"Process%d sends message m%d%d to process%d at %d:%d, tuples sending = %s, vc: %s\n",index,index,counter,nodes[random_index],ltm->tm_min,ltm->tm_sec,tuples_send_arr, cstr);

		   	

		   	lastSent[random_index+1] = vClock[index];
		   	

		   	write(sock[random_index], hello, strlen(hello));
		   	
    	}
    	mtx->unlock();
    	this_thread::sleep_for(chrono::milliseconds((int)run_exp(lambda)));

    }


    for(int i=0;i<max_connection;i++)
    	close(sock[i]);

    fprintf(fp,"Sender exiting for index = %d\n", index);
    return;
}

void fun(int index, std::vector <int> nodes, int n, int lambda, int m, int maxClients, int alpha1, int alpha2)
{
	
	int *vClock;
	vClock = new int[n+1];
	
	int *lastSent, *lastUpdate;
	lastSent = new int[n+1];
	lastUpdate = new int[n+1];

	for(int i=1;i<=n;i++)
	{
		lastSent[i]=0;
		lastUpdate[i]=0;
		vClock[i]=0;
	}

	std::mutex mtx;
	std::thread r(receiver, index, maxClients, nodes, vClock,n, &mtx,m,lastSent, lastUpdate);
	
	std::thread s(sender, index, nodes, n, vClock, &mtx,m, lastSent, lastUpdate, lambda, alpha1, alpha2);

	while(total>0);

	r.join();
	s.join();
	
}

int main(int argc, char *argv[])
{
	srand (time(NULL));
	fp = fopen("output_SK.txt", "w+");
	ifstream inputfile;
	inputfile.open("inp-params.txt");

	int n, lambda, m;
	float alpha;
	inputfile >> n >> lambda >> alpha >> m;

	int mul = 1;
	std::string conv_float=std::to_string(alpha);
	
	int check=0;
	for(int z=conv_float.length()-1;z>=0;z--)
	{
		if(conv_float[z] != '0')
			check = 1;
		if(check)
		{
			if(conv_float[z] != '.' )
				mul*=10;
			else
				break;
		}
		
	}
	double apha1 = alpha*mul;
	int alpha1 = (int)apha1;
	int alpha2 = alpha1 + mul;
	tuples_counter = 0;
	std::vector <int> graph[n+1]; 
	
	total = m*n;
	int position=0,value;
	std::string temp;
	
	for(int i=0;i<=n;i++)
	{	
		getline(inputfile, temp);
		if(i>0)
		{
			// position++;
			// for(int j=2;j<temp.length();j+=2)
			// {
			// 	value = temp[j]-48;
			// 	graph[position].push_back(value);
			// }
			position++;
			int temp_val = 0, check_val = 0;
			for(int j=0;j<temp.length();j++)
			{
				if(temp[j] == ' ' && check_val == 0)
				{
					check_val = 1;
					continue;
				}
				if(check_val == 1)
				{
					if(temp[j] == ' ')
					{
						graph[position].push_back(temp_val);
						temp_val = 0;
					}
					else
					{
						temp_val = temp_val*10 + (int)temp[j] - 48;
					}
				}
			}
			if(temp_val != 0)
				graph[position].push_back(temp_val);
		}
	}
	

	// for(int i = 1;i<=n;i++)
	// {
	// 	for(int j=0;j<graph[i].size(); j++)
	// 		cout << graph[i][j] << " ";
	// 	cout << endl;
	// }
	
	int *fill_maxClients;
	fill_maxClients = new int[n+1];

	for(int i=1;i<=n;i++)
		fill_maxClients[i]=0;

	for(int i=1;i<=n;i++)
	{
		for(int j=0;j<graph[i].size();j++)
			fill_maxClients[graph[i][j]]++;
	}

	check_sleep = n;

	std::thread th[n];
	for(int i=0;i<n;i++)
		th[i] = std::thread(fun, i+1, graph[i+1],n,lambda,m,fill_maxClients[i+1], alpha1, alpha2);

	for(int i=0;i<n;i++)
		th[i].join();

	int tuples_count = tuples_counter;
	fprintf(fp,"\nTotal space for vector clocks storage in this method = %d blocks\n",3*n);
	fprintf(fp,"\nTotal tuples send in this method = %d\n",tuples_count);
	
	return 0;
}