#ifndef __UTIL_H
#define __UTIL_H

#include <stdio.h>
#include <string.h>

extern int tracer_rank;
extern int MPI_program_warning_enable;
extern int issue_found_flag;

static int inline __split(char dst[][64], char* str, const char* spl)
{
    int n = 0;
    char *result = NULL;
    result = strtok(str, spl);
    while( result != NULL )
    {
        strcpy(dst[n++], result);
        result = strtok(NULL, spl);
    }
    return n;
}
struct list_head {
    struct list_head *next, *prev;
};

#define LIST_HEAD_INIT(name) { &(name), &(name) }
#define list_entry(ptr,type,member) container_of(ptr,type,member)
#define __offsetof(TYPE,MEMBER) ((size_t)&((TYPE *)0)->MEMBER)
#define container_of(ptr,type,member) ( {\
	const typeof( ((type*)0)->member ) *__mptr=(ptr);\
	(type*)( (char*)__mptr - __offsetof(type,member) );} )

#define list_for_each(pos, head) for(pos = (head)->next;pos != (head); pos = pos->next)

#ifdef DEBUG
#define MAX_HASH 20
#else
#define MAX_HASH 1024
#endif

static inline void INIT_LIST_HEAD(struct list_head *list){
    list->next = list;
    list->prev = list;
}

static inline void __list_add( struct list_head *new, struct list_head *prev, struct list_head *next){
    next->prev = new;
    new->next = next;
    new->prev = prev;
    prev->next = new;
}

static inline void list_add(struct list_head *new, struct list_head *head){
    __list_add(new, head, head->next);
}

static inline void list_add_tail(struct list_head *new, struct list_head *head){
    __list_add(new, head->prev, head);
}

static inline int list_empty(const struct list_head *head){
    return head->next == head;
}

static inline void __list_del(struct list_head * prev, struct list_head * next){
    next->prev = prev;
    prev->next = next;
}

static inline void list_del(struct list_head *entry){
    __list_del(entry->prev, entry->next);
    entry->next = entry;
    entry->prev = entry;
}


inline int cal_hash(long ptr){
    return (long)ptr%MAX_HASH;
}

#define DEFINE_INIT_POOL_FN(my_init_pool_func,type,init_cb) \
                        void my_init_pool_func(struct list_head* pool_head,int n){\
                        type* pool; \
                        int i; \
                        pool=(type*)malloc(n*sizeof(type));   \
                        for(i=0;i<n;i++){\
                            init_cb(&pool[i]);\
                            list_add(&(pool[i].node),pool_head);\
                        }\
                    }

#define init_pool(pool_head,type,n,init_cb) ({\
                    type* pool;\
                    int i;\
                    pool=(type*)malloc(n*sizeof(type));   \
                    for(i=0;i<n;i++){\
                        init_cb(&pool[i]);\
                        list_add(&(pool[i].node),(pool_head));\
                    }\
                })

#define __pool_pop(pool_head,type) ({\
                type* obj; \
                obj = list_entry((pool_head)->next,type,node);\
                list_del(&(obj->node)); \
                obj; \
                })
#define pool_pop(pool_head,type) \
                (list_empty(pool_head))? NULL:__pool_pop(pool_head,type);

#define pool_push(pool_head,obj) ({list_add_tail(&((obj)->node),(pool_head));})                

#define hash_add_obj(obj,table,type,conflict_fn) ({\
    int idx=cal_hash((obj)->key);\
    struct list_head* head=&(table[idx].head);\
    struct list_head* list;\
    type* item;\
    int flag=0;\
    list_for_each(list,head){\
        item=list_entry(list,type,node);\
        if(item->key==(obj)->key){\
            conflict_fn(obj,item);\
            flag=1;\
            break;\
        }\
    }\
    if(!flag)\
        list_add_tail(&(obj->node),head);\
})

#define hash_find_obj(k,table,type) ({\
    int idx=cal_hash(k);\
    struct list_head* head=&(table[idx].head);\
    struct list_head* list;\
    type* obj;\
    type* ret=NULL;\
    list_for_each(list,head){\
         obj=list_entry(list,type,node);\
         if(obj->key==k){ \
             ret=obj;\
             break;\
         }\
    }\
    ret; \
    } )

#define recycle_request_node(obj,pool_head) ({\
        list_del(&((obj)->node));\
        pool_push(pool_head,obj);\
        })

struct htable_node{
    struct list_head head;
};

void init_htable(struct htable_node* table){
    int i;
    for(i=0;i<MAX_HASH;i++){
      INIT_LIST_HEAD(&(table[i].head));
    }
}

#define view_htable(table,type,display_fn)({\
    int i;\
    struct list_head* list;\
    struct list_head* head;\
    type* obj;\
    int count=0;\
    if(tracer_rank>0) return;\
    for(i=0;i<MAX_HASH;i++){\
      count=0;\
      head=&(table[i].head);\
      printf("%d\t",i);\
      list_for_each(list,head){\
          count++;\
          obj=list_entry(list,request_node_t,node);\
          display_fn(obj);\
      }\
      printf("%d\n",count);\
    }\
})


double GHZ=-1;

double tsc_timer(){
    unsigned long var;
    unsigned int hi, lo;
    __asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long)hi << 32) | lo;
    double time=var/GHZ*1e-9;
    return (time);
}

double gettimeofday_timer()
{
    struct timeval             tp;
    (void) gettimeofday( &tp, NULL );

    return ( (double) tp.tv_sec  + ((double)tp.tv_usec / 1000000.0 )) ;
}


#endif
